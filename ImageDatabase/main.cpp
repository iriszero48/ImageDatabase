#include <unordered_map>
#include <filesystem>
#include <utility>
#include <fstream>
#include <exception>
#include <optional>
#include <thread>
#include <string>
#include <chrono>

#include <zip.h>
#include <boost/context/continuation.hpp>

extern "C" {
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/pixdesc.h>
#include <libswscale/swscale.h>
}

#include "Arguments.h"
#include "Convert.h"
#include "ImageDatabase.h"
#include "StdIO.h"
#include "Thread.h"
#include "Log.h"
#include "Algorithm.h"
#include "File.h"

ArgumentOption(Operator, build, query)

#define CatchEx

struct LogMsg
{
	decltype(std::chrono::system_clock::now()) Time;
	char* File;
	uint64_t Line;
	char* Function;
	std::string Message;
};

static Logger<LogMsg> Log;

void DealFile(const std::filesystem::path& path)
{

}

void DealFile(const uint8_t* path, uint64_t len)
{
	
}

void DealPath(const std::filesystem::path& path)
{
	const auto ext = path.extension().u8string();
	static const std::unordered_set<std::string> CompExt { ".zip", ".7z", ".rar", ".gz", ".xz" };
	if (CompExt.find(ext) != CompExt.end())
	{
		const auto zipFile = File::ReadToEnd(path);

		std::string errLog;
		zip_error_t error;
		zip_source_t* src = zip_source_buffer_create(zipFile.data(), zipFile.length(), 0, &error);
		if (src == nullptr && error.zip_err != ZIP_ER_OK)
		{
			Log.Write<LogLevel::Error>(String::FormatWstr("[DealPath] load file: {} {}", file.path().wstring(), error.str));
			Log.Write<LogLevel::Error>(std::wstring(L"load file: ") + file.path().wstring() + std::filesystem::path(error.str).wstring());
			continue;
		}

										zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &error);
										if (za == nullptr && error.zip_err != ZIP_ER_OK)
										{
											Log.Write<LogLevel::Error>(L"load file: " + file.path().wstring() + *Convert::ToWString(error.zip_err));
											continue;
										}

										const auto entries = zip_get_num_entries(za, 0);
										if (entries < 0)
										{
											Log.Write<LogLevel::Error>(L"load file: " + *Convert::ToWString(zip_get_error(za)->str));
											zip_close(za);
											continue;
										}

										for (int i = 0; i < entries; ++i)
										{
											struct zip_stat zs;
											if (zip_stat_index(za, i, 0, &zs) == 0)
											{
												if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
												{
													auto* const zf = zip_fopen_index(za, i, 0);
													if (zf == nullptr
														&& zip_get_error(za)->zip_err != ZIP_ER_OK)
													{
														Log.Write<LogLevel::Error>(L"load file: zip_fopen_index: fail");
														continue;
													}
													const auto buf = std::make_unique<char[]>(zs.size);
													if (zip_fread(zf, buf.get(), zs.size) < 0)
													{
														Log.Write<LogLevel::Error>(L"load file: zip_fread: fail");
														zip_fclose(zf);
														continue;
													}
													img = ImageDatabase::Image(file.path() / filename, std::string(buf.get(), zs.size));
													zip_fclose(zf);

													sink = sink.resume();
												}
											}
										}
										zip_close(za);
	}
	else
	{
		DealFile(path);
	}
}

void ScanFiles(const std::filesystem::path& path)
{
	for (const auto& file : std::filesystem::recursive_directory_iterator(path))
	{
		if (file.is_regular_file()) DealPath(file.path());
	}
}

int main(int argc, char* argv[])
{
#define ArgumentsFunc(arg) [&](decltype(arg)::ConvertFuncParamType value) -> decltype(arg)::ConvertResult
	
	ArgumentsParse::Argument<Operator> opArg
	{
		"",
		"operator " + OperatorDesc(),
		ArgumentsFunc(opArg) { return { *ToOperator(std::string(value)), {} }; }
	};
	ArgumentsParse::Argument<std::filesystem::path> dbArg
	{
		"-d",
		"database path"
	};
	ArgumentsParse::Argument<std::filesystem::path> pathArg
	{
		"-i",
		"input path"
	};
	ArgumentsParse::Argument<LogLevel> logLevelArg
	{
		"--loglevel",
		"log level " + LogLevelDesc(ToString(LogLevel::Info)),
		LogLevel::Info,
		ArgumentsFunc(logLevelArg) { return { *ToLogLevel(std::string(value)), {} };	}
	};
	ArgumentsParse::Argument<std::filesystem::path> logFileArg
	{
		"--logfile",
		"log level " + LogLevelDesc(ToString(LogLevel::Info)),
		""
	};
#undef ArgumentsFunc
	
	ArgumentsParse::Arguments args;
	args.Add(opArg);
	args.Add(dbArg);
	args.Add(pathArg);
	args.Add(logLevelArg);
	args.Add(logFileArg);

#ifdef CatchEx
	try
#endif
	{
		std::thread logThread;
		args.Parse(argc, argv);

		logThread = std::thread([](const LogLevel& level, std::filesystem::path logFile)
		{
			cv::redirectError([](const int status, const char* funcName, const char* errMsg,
			                     const char* fileName, const int line, void*)
			{
				auto msg =
					L"[" +
						*Convert::ToWString(fileName) + L":" +
						*Convert::ToWString(funcName) + L":" +
						*Convert::ToWString(line) +
					L"] " +
					L"status: " + *Convert::ToWString(status) + L": " +
					*Convert::ToWString(errMsg);
				if (const auto pos = msg.find_last_of(L'\n'); pos != std::wstring::npos) msg.erase(msg.find_last_of(L'\n'));
				Log.Write<LogLevel::Error>(msg);
				return 0;
			});

			av_log_set_level([&]()
			{
				switch (level)
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
				static char buf[4096]{ 0 };
				int ret = 1;
				av_log_format_line2(avc, ffLevel, fmt, vl, buf, 4096, &ret);
				auto data = *Convert::ToWString(static_cast<char*>(buf));
				if (const auto pos = data.find_last_of(L'\n'); pos != std::wstring::npos) data.erase(pos);
				if (ffLevel <= 16) Log.Write<LogLevel::Error>(data);
				else if (ffLevel <= 24) Log.Write<LogLevel::Warn>(data);
				else if (ffLevel <= 32) Log.Write<LogLevel::Log>(data);
				else Log.Write<LogLevel::Debug>(data);
			});

			Log.level = level;
			std::ofstream fs;
			if (!logFile.empty())
			{
				fs.open(logFile);
				const auto buf = std::make_unique<char[]>(4096);
				fs.rdbuf()->pubsetbuf(buf.get(), 4096);
				if (!fs)
				{
					Log.Write<LogLevel::Error>(L"log file: " + logFile.wstring() + L": open fail");
					logFile = "";
				}
			}

			while (true)
			{
				const auto [level, msg] = Log.Chan.Read();
				std::string out;
				String::StringCombine(out, "[", ToString(level), "] ");
				auto utf8 = false;
				try
				{
					String::StringCombine(out, std::filesystem::path(msg).string());
				}
				catch (...)
				{
					utf8 = true;
					String::StringCombine(out, std::filesystem::path(msg).u8string());
				}
				Console::WriteLine(out);
				if (!logFile.empty())
				{
					fs << (utf8 ? out : std::filesystem::path(out).u8string()) << std::endl;
					fs.flush();
				}

				if (level == LogLevel::None)
				{
					fs.close();
					break;
				}
			}
		}, args.Value(logLevelArg), args.Value(logFileArg));

		try
		{
			std::unordered_map<Operator, std::function<void()>>
			{
				{Operator::build, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto buildPath = args.Value(pathArg);
					ImageDatabase db(dbPath);

					std::string fileData;
					ImageDatabase::Image img;

					Log.Write<LogLevel::Debug>([]()
					{
						static void* iterateData = nullptr;
						const auto encoderGetNextCodecName = []()->std::string
						{
							auto currentCodec = av_codec_iterate(&iterateData);
							while (currentCodec != nullptr)
							{
								if (!av_codec_is_decoder(currentCodec) && currentCodec->type == AVMEDIA_TYPE_VIDEO)
								{
									currentCodec = av_codec_iterate(&iterateData);
									continue;
								}
								return currentCodec->name;
							}
							return "";
						};
						std::wstring buf;
						auto dt = encoderGetNextCodecName();
						while (!dt.empty())
						{
							buf.append(*Convert::ToWString(dt.c_str()) + L" ");
							dt = encoderGetNextCodecName();
						}
						return buf;
					}());

					std::string curFile{};
					boost::context::continuation scanFile = boost::context::callcc([&](boost::context::continuation&& sink)
					{
						for (const auto& file : std::filesystem::recursive_directory_iterator(buildPath))
						{
							if (file.is_regular_file())
							{
								curFile = file.path();
								sink = sink.resume();
							}
						}
					});


					boost::context::continuation readFile = boost::context::callcc(
						[&](boost::context::continuation&& sink)
						{
							
						}
					);

					boost::context::continuation source = boost::context::callcc(
						[&](boost::context::continuation&& sink)
						{
							for (const auto& file : std::filesystem::recursive_directory_iterator(buildPath))
							{
								if (file.is_regular_file())
								{
									const auto& filePath = file.path();
									Log.Write<LogLevel::Log>(L"Scan file: " + filePath.wstring());
									if (filePath.extension() == ".zip")
									{
										const auto zipFile = *OpenCvUtility::ReadToEnd(file.path());

										zip_error_t error;
										zip_source_t* src = zip_source_buffer_create(zipFile.data(), zipFile.length(), 0, &error);
										if (src == nullptr && error.zip_err != ZIP_ER_OK)
										{
											Log.Write<LogLevel::Error>(std::wstring(L"load file: ") + file.path().wstring() + std::filesystem::path(error.str).wstring());
											continue;
										}

										zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &error);
										if (za == nullptr && error.zip_err != ZIP_ER_OK)
										{
											Log.Write<LogLevel::Error>(L"load file: " + file.path().wstring() + *Convert::ToWString(error.zip_err));
											continue;
										}

										const auto entries = zip_get_num_entries(za, 0);
										if (entries < 0)
										{
											Log.Write<LogLevel::Error>(L"load file: " + *Convert::ToWString(zip_get_error(za)->str));
											zip_close(za);
											continue;
										}

										for (int i = 0; i < entries; ++i)
										{
											struct zip_stat zs;
											if (zip_stat_index(za, i, 0, &zs) == 0)
											{
												if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
												{
													auto* const zf = zip_fopen_index(za, i, 0);
													if (zf == nullptr
														&& zip_get_error(za)->zip_err != ZIP_ER_OK)
													{
														Log.Write<LogLevel::Error>(L"load file: zip_fopen_index: fail");
														continue;
													}
													const auto buf = std::make_unique<char[]>(zs.size);
													if (zip_fread(zf, buf.get(), zs.size) < 0)
													{
														Log.Write<LogLevel::Error>(L"load file: zip_fread: fail");
														zip_fclose(zf);
														continue;
													}
													img = ImageDatabase::Image(file.path() / filename, std::string(buf.get(), zs.size));
													zip_fclose(zf);

													sink = sink.resume();
												}
											}
										}
										zip_close(za);
									}
									else if (const auto ext = filePath.extension();
										ext == ".gif" ||
										ext == ".mp4" ||
										ext == ".mkv" ||
										ext == ".flv" ||
										ext == ".avi" ||
										ext == ".mpg" ||
										ext == ".vob" ||
										ext == ".mov" ||
										ext == ".wmv" ||
										ext == ".swf" ||
										ext == ".3gp" ||
										ext == ".mts" ||
										ext == ".rm" ||
										ext == ".ts" ||
										ext == ".m2ts" ||
										ext == ".rmvb" ||
										ext == ".mpeg" ||
										ext == ".webm")
									{
										std::string gifPath = filePath.u8string();

										AVFormatContext* fmtCtx = nullptr;
										AVCodec* codec = nullptr;
										AVCodecContext* codecCtx = nullptr;
										AVFrame* frame = nullptr;
										AVFrame* decFrame = nullptr;
										SwsContext* swsCtx = nullptr;

										try
										{
											int ret;
											ret = avformat_open_input(&fmtCtx, gifPath.c_str(), nullptr, nullptr);
											if (ret < 0) throw std::runtime_error("avforamt_open_input fail: " + *Convert::ToString(ret));

											ret = avformat_find_stream_info(fmtCtx, nullptr);
											if (ret < 0) throw std::runtime_error("avformat_find_stream_info fail: " + *Convert::ToString(ret));

											ret = av_find_best_stream(fmtCtx, AVMEDIA_TYPE_VIDEO, -1, -1, &codec, 0);
											if (ret < 0) throw std::runtime_error("av_find_best_stream fail: " + *Convert::ToString(ret));

											const int streamId = ret;
											auto codecParams = fmtCtx->streams[streamId]->codecpar;

											codecCtx = avcodec_alloc_context3(codec);
											if (!codecCtx) throw std::runtime_error("avcodec_alloc_context3 fail");

											avcodec_parameters_to_context(codecCtx, codecParams);
											ret = avcodec_open2(codecCtx, codec, nullptr);
											if (ret < 0) throw std::runtime_error("avcodec_open2 fail: " + *Convert::ToString(ret));

											const int dstWidth = codecParams->width;
											const int dstHeight = codecParams->height;
											const AVPixelFormat dstPixFmt = AV_PIX_FMT_BGR24;
											if (codecCtx->pix_fmt != AV_PIX_FMT_NONE)
											{
												swsCtx = sws_getCachedContext(
													nullptr, codecParams->width, codecParams->height, codecCtx->pix_fmt,
													dstWidth, dstHeight, dstPixFmt, 0, nullptr, nullptr, nullptr);
												if (!swsCtx) throw std::runtime_error("sws_getCachedContext fail");
											}

											frame = av_frame_alloc();
											std::string frameBuf(av_image_get_buffer_size(dstPixFmt, dstWidth, dstHeight, dstWidth), 0);
											av_image_fill_arrays(
												frame->data, frame->linesize,
												reinterpret_cast<uint8_t*>(frameBuf.data()),
												dstPixFmt, dstWidth, dstHeight, dstWidth);
						
											decFrame = av_frame_alloc();
											bool eof = false;
											AVPacket* pkt = av_packet_alloc();
											do
											{
												if (!eof)
												{
													ret = av_read_frame(fmtCtx, pkt);
													if (ret < 0 && ret != AVERROR_EOF) throw std::runtime_error("av_read_frame fail: " + *Convert::ToString(ret));
													
													if (ret == 0 && pkt->stream_index != streamId)
													{
														av_packet_unref(pkt);
														continue;
													}
													eof = (ret == AVERROR_EOF);
												}
												if (eof)
												{
													av_packet_unref(pkt);
													ret = 0;
												}
												else {ret = avcodec_send_packet(codecCtx, pkt);
												if (ret < 0) throw std::runtime_error("avcodec_send_packet: error sending a packet for decoding: " + *Convert::ToString(ret));
												}
												while (ret >= 0)
												{
													ret = avcodec_receive_frame(codecCtx, decFrame);
													if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF)
													{
														av_packet_unref(pkt);
														break;
													}
													if (ret < 0) throw std::runtime_error("avcodec_receive_frame: error during decoding: " + *Convert::ToString(ret));

													if (swsCtx == nullptr)
													{
														swsCtx = sws_getCachedContext(
															nullptr, codecParams->width, codecParams->height, codecCtx->pix_fmt,
															dstWidth, dstHeight, dstPixFmt, 0, nullptr, nullptr, nullptr);
														if (!swsCtx) throw std::runtime_error("sws_getCachedContext fail");
													}
							
													sws_scale(swsCtx, decFrame->data, decFrame->linesize, 0, decFrame->height, frame->data, frame->linesize);

													cv::Mat image(dstHeight, dstWidth, CV_8UC3, (frameBuf.data()), frame->linesize[0]);
													//cv::imshow("", image);
													//cv::waitKey(0);
													std::vector<uchar> rawData;
													imencode(".bmp", image, rawData);
													std::string dataStr;
													std::copy_n(rawData.begin(), rawData.size(), std::back_inserter(dataStr));
													rawData.clear();
													rawData.shrink_to_fit();
													const auto subPath = filePath / *Convert::ToString(codecCtx->frame_number);
													Log.Write<LogLevel::Info>(L"load file: " + subPath.wstring());
													img = ImageDatabase::Image(subPath, dataStr);
													sink = sink.resume();
												}
												av_packet_unref(pkt);
											} while (!eof);
											av_packet_free(&pkt);
										}
										catch (const std::exception& ex)
										{
											Log.Write<LogLevel::Error>(*Convert::ToWString(ex.what()) + L": " + filePath.wstring());
										}
										if (decFrame != nullptr) av_frame_free(&decFrame);
										if (frame != nullptr) av_frame_free(&frame);
										if (codecCtx != nullptr) avcodec_free_context(&codecCtx);
										if (fmtCtx != nullptr) avformat_close_input(&fmtCtx);
										if (swsCtx != nullptr) sws_freeContext(swsCtx);
									}
									else if (OpenCvUtility::IsImage(filePath.extension().string()))
									{
										Log.Write<LogLevel::Info>(L"load file: " + file.path().wstring());
										img = ImageDatabase::Image(file.path());
										sink = sink.resume();
									}
									else
									{
										Log.Write<LogLevel::Warn>(L"Unsupported format: " + file.path().wstring());
									}
								}
							}
							img = ImageDatabase::Image{};
							return std::move(sink);
						});

					for (uint64_t i = 0; !img.Path.empty(); source = source.resume(), ++i)
					{
						Log.Write<LogLevel::Info>(*Convert::ToWString(Convert::ToString(i)->c_str()) + L": compute md5: " + img.Path.wstring());
						img.ComputeMd5();
						Log.Write<LogLevel::Info>(L"compute vgg16: " + img.Path.wstring());
						try
						{
							img.ComputeVgg16();
							Log.Write<LogLevel::Info>(L"file: " + img.Path.wstring() + L": vgg16 start with: " + *Convert::ToWString(img.Vgg16[0]));
						}
						catch (const cv::Exception& ex)
						{
							Log.Write<LogLevel::Error>(L"compute vgg16: " + img.Path.wstring() + L": " + *Convert::ToWString(ex.what()));
						}
						img.FreeMemory();
						db.Images.push_back(img);
					}

					Log.Write<LogLevel::Info>(L"save database: " + dbPath.wstring());
					db.Save(dbPath);
					Log.Write<LogLevel::Info>(L"database size: " + *Convert::ToWString(Convert::ToString(db.Images.size())->c_str()));
				}},
				{Operator::query, [&]()
				{
					const auto dbPath = args.Value(dbArg);
					const auto input = args.Value(pathArg);
					ImageDatabase db(dbPath);
					Log.Write<LogLevel::Info>(L"load database: " + dbPath.wstring());
					db.Load(dbPath);
					Log.Write<LogLevel::Info>(L"database size: " + *Convert::ToWString(db.Images.size()));

					Log.Write<LogLevel::Info>(L"load database: " + input.wstring());
					ImageDatabase::Image img(input);
					Log.Write<LogLevel::Info>(L"compute md5: " + input.wstring());
					img.ComputeMd5();
					Log.Write<LogLevel::Info>(L"compute vgg16: " + input.wstring());
					img.ComputeVgg16();
					Log.Write<LogLevel::Info>(L"file: " + img.Path.wstring() + L": vgg16 start with: " + *Convert::ToWString(img.Vgg16[0]));
					img.FreeMemory();
					
					Log.Write<LogLevel::Info>(L"search start ...");
					Algorithm::Sort<true>(db.Images.begin(), db.Images.end(), [&](const ImageDatabase::Image& a, const ImageDatabase::Image& b)
					{
						return std::greater()(a.Vgg16.dot(img.Vgg16), b.Vgg16.dot(img.Vgg16));
					});

					Log.Write<LogLevel::Info>(L"search done.");
					for (const auto& i : db.Images)
					{
						if (const auto v = i.Vgg16.dot(img.Vgg16); v >= 0.8f)
						{
							Log.Write<LogLevel::Log>(L"found " + std::filesystem::path(*Convert::ToString(v)).wstring() + L": " + i.Path.wstring());
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
			Log.Write<LogLevel::Error>(*Convert::ToWString(ex.what()));
		}

		Log.Write<LogLevel::None>(L"{ok}.");
		logThread.join();
	}
#ifdef CatchEx
	catch (const std::exception& e)
	{
		Console::Error::WriteLine(e.what());
		Console::Error::WriteLine(args.GetDesc());
	}
#endif
}
