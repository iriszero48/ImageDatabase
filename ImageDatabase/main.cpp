#include <unordered_map>
#include <filesystem>
#include <utility>
#include <fstream>
#include <exception>
#include <optional>
#include <thread>
#include <string>
#include <chrono>
#include <regex>

// extern "C"
// {
// #include <libavutil/avutil.h>
// }

// #include "Arguments.h"
// #include "Convert.h"
// #include "ImageDatabase.h"
// #include "StdIO.h"
// #include "Thread.h"
// #include "Log.h"
// #include "Algorithm.h"
// #include "Requires.h"
// #include "Time.h"
// #include "Function.h"

#include "Enum/Enum.hpp"
#include "Arguments/Arguments.hpp"

#include "ImageDatabase.hpp"
#include "StdIO/StdIO.hpp"

#include "Algorithm.hpp"

#include <nlohmann/json.hpp>
#include <range/v3/view.hpp>

MakeEnum(Operator, build, query);

struct BuildParam
{
	std::filesystem::path DbPath;
	ImageDatabase::Device Dev;
	std::filesystem::path BuildPath;
	std::unordered_set<std::u8string> ExtBlackList;
	std::unordered_map<std::u8string, ImageDatabase::Decoder> ExtDecoderList;
	size_t ThreadNum;
	std::unordered_set<std::u8string> ZipList;
};

template<bool Mt = false>
void BuildCore(const BuildParam& param)
{
	ImageDatabase::Database db{};
	if (exists(param.DbPath)) db.Load(param.DbPath);
	LogInfo("database size: {}", db.Images.Data.size());

	if constexpr (Mt)
	{
		CuThread::Channel<std::optional<ImageDatabase::Reader::LoadType>, CuThread::Dynamics> mq{};
		mq.DynLimit = param.ThreadNum;
		CuThread::Synchronize push([&]<typename T0, typename T1, typename T2>(T0 && p, T1 && m, T2 && v) {
			db.Images.Set(
				std::forward<T0>(p), std::forward<T1>(m), std::forward<T2>(v));
		});

		ImageDatabase::Reader reader([&](ImageDatabase::Reader::LoadType&& data)
			{
				mq.Write(std::move(data));
			}, param.BuildPath);
		reader.ExtBlackList = param.ExtBlackList;
		reader.ExtDecoderList = param.ExtDecoderList;
		reader.ZipList = param.ZipList;

		std::vector<std::thread> threads(param.ThreadNum);
		for (auto& t : threads)
		{
			t = std::thread([&]
				{
					ImageDatabase::Extractor extractor(param.Dev);

					while (true)
					{
						auto i = mq.Read();
						if (!i) break;

						LogInfo("scan: {}", std::visit(CuUtility::Variant::Visitor{
							[](const ImageDatabase::Reader::PathType& info)
							{
								return info;
							},
								[](const ImageDatabase::Reader::MemoryType& info)
							{
								return info.Path;
							}
						}, * i));
						reader.Process([&]<typename T0, typename T1, typename T2>(T0 && p, T1 && m, T2 && v) {
							push(
								std::forward<T0>(p), std::forward<T1>(m),
								std::forward<T2>(v));
						}, extractor, * i);
					}
				});
		}

		reader.Read();

		for (auto& _ : threads) mq.Write({});
		for (auto& t : threads) t.join();
	}
	else
	{
		ImageDatabase::Extractor extractor(param.Dev);
		ImageDatabase::Reader reader([&](ImageDatabase::Reader::LoadType&& data)
			{
				LogInfo("scan: {}", std::visit(CuUtility::Variant::Visitor{
					[](const ImageDatabase::Reader::PathType& info)
					{
						return info;
					},
						[](const ImageDatabase::Reader::MemoryType& info)
					{
						return info.Path;
					}
				}, data));
				reader.Process([&]<typename T0, typename T1, typename T2>(T0&& p, T1&& m, T2&& v) {
					db.Images.Set(std::forward<T0>(p), std::forward<T1>(m), std::forward<T2>(v));
				}, extractor, data);
			}, param.BuildPath);
		reader.ExtBlackList = param.ExtBlackList;
		reader.ExtDecoderList = param.ExtDecoderList;
		reader.ZipList = param.ZipList;

		reader.Read();
	}

	LogInfo("save database : {}", param.DbPath);
	db.Save(param.DbPath);
	LogInfo("database size: {}", db.Images.Data.size());
}

int main(int argc, const char* argv[])
{
	Args::EnumArgument<Operator, true> opArg
	{
		"",
		"operator " + String::ToString(Enum::StringValues<Operator>())
	};
	Args::Argument<std::filesystem::path, 1, true> dbArg
	{
		"-d",
		"database path"
	};
	Args::Argument<std::filesystem::path> pathArg
	{
		"-i",
		"input path"
	};
	Args::Argument<double> thresholdArg
	{
		"-t",
		"threshold [0.0, 1.0] {0.8}",
		0.8f,
		[](const auto& value)
		{
			const auto val = *Convert::FromString<double>(value);
			if (!(val >= 0.0f && val <= 1.0f))
				throw Args::ConvertException(CuUtility::String::Combine("[", __FUNCTION__, "] [", CuUtility_Filename, ":", CuUtility_LineString, "] out of range").data());
			return val;
		}
	};
	Args::Argument<size_t> threadLimitArg
	{
		"--threads",
		"threads (0 = hardware concurrency) (build only)",
		1,
		[](const auto& val)
		{
			const auto ret = Convert::FromString<size_t>(val).value();
			return ret == 0 ? std::thread::hardware_concurrency() : ret;
		}
	};
	Args::EnumArgument deviceArg
	{
		"--device",
		String::Format("device {}", Enum::StringValues<ImageDatabase::Device>()),
		ImageDatabase::Device::cuda
	};
	Args::Argument<std::unordered_set<std::u8string>> ignoreExtArg
	{
		"--ignore",
		R"(ignore file exts. example: "ext,...". default: "txt")",
		std::unordered_set<std::u8string>{u8".txt"},
		[](const auto& val)
		{
			std::unordered_set<std::u8string> buf;
			String::Split(String::ToU8String(val), u8',', [&](const auto& v)
				{
					buf.emplace(String::FormatU8(".{}", String::ToLowerU8(v)));
				});
			return buf;
		},
		[](const auto& val)
		{
			std::vector<std::string> ret{};
			std::transform(val.begin(), val.end(), std::back_inserter(ret), String::ToString<std::u8string>);
			return nlohmann::to_string(nlohmann::json(ret));
		}
	};
	Args::Argument<std::unordered_set<std::u8string>> zipExtArg
	{
		"--zip",
			R"(zip file exts. default: "zip,7z,rar,gz,xz")",
			std::unordered_set<std::u8string>{u8".zip", u8".7z", u8".rar", u8".gz", u8".xz"},
			[](const auto& val)
		{
			std::unordered_set<std::u8string> buf;
			String::Split(String::ToU8String(val), u8',', [&](const auto& v)
				{
					buf.emplace(String::FormatU8(".{}", String::ToLowerU8(v)));
				});
			return buf;
		},
			[](const auto& val)
		{
			std::vector<std::string> ret{};
			std::transform(val.begin(), val.end(), std::back_inserter(ret), String::ToString<std::u8string>);
			return nlohmann::to_string(nlohmann::json(ret));
		}
	};
	Args::Argument<std::unordered_map<std::u8string, ImageDatabase::Decoder>> decoderSpecArg
	{
		"--decoder",
			String::Format(R"(decoder{} spec. example: "ext=FFmpeg,...". default: "nef=DirectXTex")", Enum::StringValues<ImageDatabase::Decoder>()),
			std::unordered_map<std::u8string, ImageDatabase::Decoder>{{u8".nef", ImageDatabase::Decoder::DirectXTex}},
			[](const auto& val)
		{
			std::unordered_map<std::u8string, ImageDatabase::Decoder> buf;
			String::Split(String::ToU8String(val), u8',', [&](const auto& kvStr)
				{
					std::u8string k{};
					ImageDatabase::Decoder v{};
					String::Split(kvStr, u8'=', [&, i=0](const auto& vs) mutable
					{
						switch (i)
						{
						case 0:
							k = String::ToLowerU8(String::FormatU8(".{}", vs));
							return;
						case 1:
							v = Enum::FromString<ImageDatabase::Decoder>(String::ToString(vs));
							return;
						default:
							throw Args::ConvertException(CuUtility::String::Combine("[", __FUNCTION__, "] [", CuUtility_Filename, ":", CuUtility_LineString, "] parse error").data());
						}
					});
					buf[k] = v;
				});
			return buf;
		},
			[](const auto& val)
		{
			std::string buf = "{";
				for (const auto& [k, v] : val)
				{
					String::AppendsTo(buf, "\"", String::ToString(k), "\"", ":", Enum::ToString(v), ",");
				}
				if (buf[buf.length() - 1] == ',') buf.erase(buf.length() - 1);
				buf.append("}");
				return buf;
		}
	};
	Args::EnumArgument logLevelArg
	{
		"--loglevel",
			String::Format("log level {}", Enum::StringValues<LogLevel>()),
		LogLevel::Info,
	};
	Args::Argument logFileArg
	{
		"--logfile",
		"log file path",
		std::filesystem::path{}
	};
	
	auto args = Args::Arguments{};
	args.Add(opArg
		,dbArg
		,pathArg
		,thresholdArg
		,threadLimitArg
		,deviceArg
		,ignoreExtArg
		,zipExtArg
		,decoderSpecArg
		,logLevelArg
		,logFileArg);

	const auto usage = [&] { return String::Format("Usage:\n{}\n{}", argv[0], args.GetDesc()); };

	try
	{
		args.Parse(argc, argv);
	}
	catch (const std::exception& ex)
	{
		Console::Error::WriteLine(ex.what());
		Console::Error::WriteLine(usage());
	}

		std::thread logThread;
		const auto logLevel = args.Value(logLevelArg);
		ImageDatabase::Log.Level = logLevel;
		LogInfo("\n{}", args.GetValuesDesc());

		av_log_set_level([&]()
		{
			switch (logLevel)
			{
			case LogLevel::None:
				return AV_LOG_QUIET;
			case LogLevel::Error:
				return AV_LOG_ERROR;
			case LogLevel::Warn:
				return AV_LOG_WARNING;
			case LogLevel::Debug:
				return AV_LOG_VERBOSE;
			default:
				return AV_LOG_INFO;
			}
		}());

		av_log_set_callback([](void* avc, int ffLevel, const char* fmt, va_list vl)
		{
			LogLevel outLv = LogLevel::Info;
			if (ffLevel <= AV_LOG_ERROR) outLv = LogLevel::Error;
			else if (ffLevel <= AV_LOG_WARNING) outLv = LogLevel::Warn;
			else if (ffLevel <= AV_LOG_INFO) outLv = LogLevel::Info;
			else if (ffLevel <= AV_LOG_TRACE) outLv = LogLevel::Debug;

			if (outLv <= ImageDatabase::Log.Level)
			{
				int bufSize = 4096;
				std::unique_ptr<char[]> buf;

				while (true)
				{
					buf = std::make_unique<char[]>(bufSize);
					int prefix = 1;
					const auto ret = av_log_format_line2(avc, ffLevel, fmt, vl, buf.get(), bufSize, &prefix);
					if (ret < 0)
					{
						LogErr("[av_log_format_line2] {}", CuVid::_Detail::AV::AvStrError(ret));
						return;
					}
					if (ret < bufSize)
					{
						break;
					}

					bufSize *= 2;
				}

				std::u8string_view bv(reinterpret_cast<const char8_t*>(buf.get()));
				switch (outLv)
				{
				case LogLevel::Error:
					LogErr("{}", bv);
					break;
				case LogLevel::Warn:
					LogWarn("{}", bv);
					break;
				case LogLevel::Verb:
					LogVerb("{}", bv);
					break;
				case LogLevel::Debug:
					LogDebug("{}", bv);
					break;
				default:
					LogInfo("{}", bv);
					break;
				}
			}
		});

		cv::redirectError([](const int status, const char* funcName, const char* errMsg,
			const char* fileName, const int line, void*)
		{
			LogErr("[{}:{}] [{}] error {}: {}", fileName, line, funcName, status, errMsg);
			return 0;
		});
		
		logThread = std::thread([](const LogLevel& level, std::filesystem::path logFile)
		{
			std::ofstream fs;
			if (!logFile.empty())
			{
				fs.open(logFile);
				if (!fs)
				{
					LogErr("log file '{}': open fail", logFile.u16string());
					logFile = "";
				}
			}

			std::unordered_map<LogLevel, Console::Color> colorMap
			{
				{ LogLevel::None, Console::Color::White },
				{ LogLevel::Error, Console::Color::Red },
				{ LogLevel::Warn, Console::Color::Yellow },
				{ LogLevel::Info, Console::Color::White },
				{ LogLevel::Verb, Console::Color::Gray },
				{ LogLevel::Debug, Console::Color::Blue }
			};

			while (true)
			{
				const auto [level, raw] = ImageDatabase::Log.Chan.Read();
				const auto& [time, src, msg] = raw;

				auto out = String::FormatU8("[{}] [{}] {}{}", Enum::ToString(level), ImageDatabase::Detail::LogTime(time), src, msg);
				if (out[out.length() - 1] == u8'\n') out.erase(out.length() - 1);

				SetForegroundColor(colorMap.at(level));
				Console::WriteLine(String::ToDirtyUtf8StringView(out));

				
				if (!logFile.empty())
				{
					fs << String::ToDirtyUtf8StringView(out) << std::endl;
					fs.flush();
				}

				if (level == LogLevel::None)
				{
					fs.close();
					break;
				}
			}
		}, logLevel, args.Value(logFileArg));

		try
		{
			const auto extBlackList = args.Value(ignoreExtArg);
			const auto extDecoderList = args.Value(decoderSpecArg);
			const auto zipList = args.Value(zipExtArg);
			std::unordered_map<Operator, std::function<void()>>
			{
				{Operator::build, [&]
				{
					const BuildParam param
						{
							args.Value(dbArg),
							args.Value(deviceArg),
							args.Value(pathArg),
							extBlackList,
							extDecoderList,
							args.Value(threadLimitArg),
							zipList
						};
					if (param.ThreadNum == 1)
					{
						BuildCore<false>(param);
					}
					else
					{
						BuildCore<true>(param);
					}
				}},
				{Operator::query, [&]
				{
					const auto dbPath = args.Value(dbArg);
					const auto input = args.Value(pathArg);
					const auto threshold = args.Value(thresholdArg);
					const auto dev = args.Value(deviceArg);

					ImageDatabase::Extractor extractor(dev);

					LogInfo("load database: {}", dbPath);
					ImageDatabase::Database<ImageDatabase::VectorContainer> db(dbPath);
					db.Images.Data.shrink_to_fit();

					LogInfo("database size: {}", db.Images.Data.size());

					LogInfo("load file: {}", input);

					ImageDatabase::VectorContainer images{};
					ImageDatabase::Reader reader([&](ImageDatabase::Reader::LoadType&& data)
					{
						if (images.Data.empty())
						{
							reader.Process([&]<typename T0, typename T1, typename T2>(T0&& p, T1&& m, T2&& v) { images.Set(
								                               std::forward<T0>(p), std::forward<T1>(m),
								                               std::forward<T2>(v)); }, extractor, data);
						}
					}, input);
					reader.ExtBlackList = extBlackList;
					reader.ExtDecoderList = extDecoderList;
					reader.ZipList = zipList;
					reader.Read();

					LogInfo("search start ...");
					const auto& img = images.Data[0];
					using CacheType = std::tuple<const ImageDatabase::ImageInfo*, float>;
					std::vector<CacheType> cache(db.Images.Data.size());
					std::transform(std::execution::par_unseq, db.Images.Data.begin(), db.Images.Data.end(), cache.begin(),
						[&](const ImageDatabase::ImageInfo& v) { return CacheType(&v, v.Vgg16.dot(img.Vgg16)); });
					
					std::sort(std::execution::par_unseq, cache.begin(), cache.end(), [&](const auto& a, const auto& b)
					{
							return std::greater()(std::get<1>(a), std::get<1>(b));
					});

					LogInfo("search done.");
					for (const auto& [pImg, val] : cache)
					{
						if (val >= threshold)
						{
							LogInfo("found '{}': {}", pImg->Path, val);
							if (const auto pv = std::u8string_view(pImg->Path.data(), pImg->Path.size()); exists(std::filesystem::path(pv)))
							{
								std::string ps{};
								try
								{
									ps = String::ToString(ps);
								}
								catch (...)
								{
								}
								cv::imshow(ps.empty() ? String::ToDirtyUtf8String(pv) : ps, val);
							}
						}
						else
						{
							break;
						}
					}
				}}
			}[args.Value(opArg)]();
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", ex.what());
			LogInfo(usage());
		}

		LogNone("{ok}.");

		logThread.join();
	
}
