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

#ifndef __cpp_lib_execution
#define ParallelUseTbb
#endif

#include "Parallel/Parallel.hpp"

#include "Enum/Enum.hpp"
#include "Arguments/Arguments.hpp"
#include "CSV/CSV.hpp"

#include "ImageDatabase.hpp"
#include "StdIO/StdIO.hpp"
#include "Enumerable/Enumerable.hpp"

#include "Algorithm.hpp"

#include <nlohmann/json.hpp>
#include <range/v3/view.hpp>
#include <boost/config.hpp>

#include <sqlite3.h>

#include <pqxx/pqxx>

#ifndef _MSC_VER
#include <unistd.h>
#include <signal.h>
#endif

CuEnum_MakeEnum(Operator, Build, Query, Concat, Export, Replace);
CuEnum_MakeEnum(ExportType, Json, JsonLines, CSV, PostgreSQL, SQLite);

#pragma region ArgsDefs
CuArgs::EnumArgument<Operator, true> OperatorArg{
		"",
		"operator " + CuStr::ToString(CuEnum::Strings<Operator>()) };

CuArgs::Argument<std::string, 1, true> DatabaseArg{
	"-d",
	"database path" };

CuArgs::Argument<std::vector<std::filesystem::path>> PathArg{
	"-i",
	"input path",
	[](const auto& v)
	{
		std::vector<std::filesystem::path> buf{};
		CuStr::Split(v, ';', [&](const auto& it)
		{
			buf.emplace_back(it);
		});
		return buf;
	},
	[](const auto& v)
	{
		return CuStr::ToString(v);
	} };

CuArgs::Argument<float> ThresholdArg{
	"-t",
	"threshold [0.0, 1.0]. default: 0.8",
	0.8f,
	[](const auto& value)
	{
		const auto val = *CuConv::FromString<float>(value);
		if (!(val >= 0.0f && val <= 1.0f))
			throw CuArgs::ConvertException(CuUtil::String::Combine("[", __FUNCTION__, "] [", CuUtil_Filename, ":", CuUtil_LineString, "] out of range").data());
		return val;
	} };

CuArgs::Argument<size_t> ThreadLimitArg{
	"--threads",
	"threads (0 = hardware concurrency) (build only). default: 1",
	1,
	[](const auto& val)
	{
		const auto ret = CuConv::FromString<size_t>(val).value();
		return ret == 0 ? std::thread::hardware_concurrency() : ret;
	} };

CuArgs::EnumArgument DeviceArg{
	"--device",
	CuStr::Format("device {}. default: {}", CuEnum::Strings<ImageDatabase::Device>(), CuEnum::ToString(ImageDatabase::Device::cuda)),
	ImageDatabase::Device::cuda };

CuArgs::Argument<std::unordered_set<std::u8string>> IgnoreExtArg{
	"--ignore",
	R"(ignore file exts. example: "ext,...". default: "txt")",
	std::unordered_set<std::u8string>{u8".txt"},
	[](const auto& val)
	{
		std::unordered_set<std::u8string> buf;
		CuStr::Split(CuStr::ToU8String(val), u8',', [&](const auto& v)
					  { buf.emplace(CuStr::FormatU8(".{}", CuStr::ToLowerU8(v))); });
		return buf;
	},
	[](const auto& val)
	{
		std::vector<std::string> ret{};
		std::transform(val.begin(), val.end(), std::back_inserter(ret), CuStr::ToString<std::u8string>);
		return nlohmann::to_string(nlohmann::json(ret));
	} };

CuArgs::Argument<std::unordered_set<std::u8string>> ZipExtArg{
	"--zip",
	R"(zip file exts. default: "zip,7z,rar,gz,xz")",
	std::unordered_set<std::u8string>{u8".zip", u8".7z", u8".rar", u8".gz", u8".xz"},
	[](const auto& val)
	{
		std::unordered_set<std::u8string> buf;
		CuStr::Split(CuStr::ToU8String(val), u8',', [&](const auto& v)
					  { buf.emplace(CuStr::FormatU8(".{}", CuStr::ToLowerU8(v))); });
		return buf;
	},
	[](const auto& val)
	{
		std::vector<std::string> ret{};
		std::transform(val.begin(), val.end(), std::back_inserter(ret), CuStr::ToString<std::u8string>);
		return nlohmann::to_string(nlohmann::json(ret));
	} };

CuArgs::Argument<std::unordered_map<std::u8string, ImageDatabase::Decoder>> DecoderSpecArg{
	"--decoder",
	CuStr::Format(R"(decoder{} spec. example: "ext=FFmpeg,...". default: "nef=DirectXTex")", CuEnum::Strings<ImageDatabase::Decoder>()),
	std::unordered_map<std::u8string, ImageDatabase::Decoder>{{u8".nef", ImageDatabase::Decoder::DirectXTex}},
	[](const auto& val)
	{
		std::unordered_map<std::u8string, ImageDatabase::Decoder> buf;
		CuStr::Split(CuStr::ToU8String(val), u8',', [&](const auto& kvStr)
					  {
				std::u8string k{};
				ImageDatabase::Decoder v{};
				CuStr::Split(kvStr, u8'=', [&, i = 0](const auto& vs) mutable
				{
					switch (i)
					{
					case 0:
						k = CuStr::ToLowerU8(CuStr::FormatU8(".{}", vs));
						return;
					case 1:
						v = CuEnum::FromString<ImageDatabase::Decoder>(CuStr::ToString(vs)).value();
						return;
					default:
						throw CuArgs::ConvertException(CuUtil::String::Combine("[", __FUNCTION__, "] [", CuUtil_Filename, ":", CuUtil_LineString, "] parse error").data());
					}
				});
				buf[k] = v; });
		return buf;
	},
	[](const auto& val)
	{
		std::string buf = "{";
		for (const auto& [k, v] : val)
		{
			CuStr::AppendsTo(buf, "\"", CuStr::ToString(k), "\"", ":", CuEnum::ToString(v), ",");
		}
		if (buf[buf.length() - 1] == ',')
			buf.erase(buf.length() - 1);
		buf.append("}");
		return buf;
	} };

CuArgs::EnumArgument LogLevelArg{
	"--loglevel",
	CuStr::Format("log level {}. default: {}", CuEnum::Strings<CuLog::LogLevel>(), CuEnum::ToString(CuLog::LogLevel::Info)),
	CuLog::LogLevel::Info,
};

CuArgs::Argument LogFileArg{
	"--logfile",
	"log file path",
	std::filesystem::path{} };

CuArgs::Argument ReplaceRegexArg{
	"--regex",
	"replace regex"
};

static std::unordered_map<char, std::string> RegexFlagStrings
{
	{ 'E', "ECMAScript"},
	{ 'b', "basic" },
	{ 'e', "extended" },
	{ 'a', "awk" },
	{ 'g', "grep" },
	{ 'G', "egrep" },
	{ 'i', "icase" },
	{ 'n', "nosubs" },
	{ 'o', "optimize" },
	{ 'c', "collate" }
};

static std::unordered_map<char, std::regex_constants::syntax_option_type> RegexFlagMap
{
	{'E', std::regex::ECMAScript},
	{ 'b', std::regex::basic },
	{ 'e', std::regex::extended },
	{ 'a', std::regex::awk },
	{ 'g', std::regex::grep },
	{ 'G', std::regex::egrep },
	{ 'i', std::regex::icase },
	{ 'n', std::regex::nosubs },
	{ 'o', std::regex::optimize },
	{ 'c', std::regex::collate },
};

CuArgs::Argument<decltype(std::regex::grep | std::regex::icase)> ReplaceRegexFlagArg{
	"--regex-flags",
	"replace regex mode (E = ECMAScript, b = basic, e = extended, a = awk, g = grep, G = egrep, i = icase, n = nosubs, o = optimize, c = collate). default: {ECMAScript}",
	std::regex::ECMAScript,
	[](const auto& v)
	{
		decltype(std::regex::grep | std::regex::icase) buf{};
		for (auto c : v)
		{
			buf |= RegexFlagMap[c];
		}
		return buf;
	},
	[](const auto& v)
	{
		std::vector<std::string> buf;

		for (auto [fk, fv] : RegexFlagMap)
		{
			if (fv & v) buf.push_back(RegexFlagStrings[fk]);
		}

		return CuStr::Join(buf.begin(), buf.end(), ", ");
	}
};

CuArgs::Argument ReplaceValueArg{
	"--value",
	"replace value",
};

CuArgs::EnumArgument<ExportType> ExportTypeArg
{
	"--export",
	CuStr::Format("export type {}", CuEnum::Strings<ExportType>())
};

CuArgs::BoolArgument LazyArg
{
	"--lazy",
	"lazy load"
};

CuArgs::Argument OutputArg
{
	"-o",
	"output path"
};

CuArgs::Argument<> OutputDatabaseNameArg
{
	"--database",
	"database name. default: images_db",
	"images_db"
};

CuArgs::Argument<> OutputTableNameArg
{
	"--table",
	"table name. default: images",
	"images"
};

CuArgs::Argument<> OutputColumnsArg
{
	"--columns",
	"columns. default: img_path,img_md5,img_vgg16",
	"img_path,img_md5,img_vgg16"
};


CuArgs::Argument<> DatabaseUserArg
{
	"--database-user",
	"database username. default: postgres",
	"postgres"
};

CuArgs::Argument<> DatabasePasswordArg
{
	"--database-password",
	"database password",
	""
};

CuArgs::BoolArgument SyncModeArg
{
	"--sync",
	"sync write"
};

CuArgs::BoolArgument NoCheckArg
{
	"--no-check",
	"no check",
	false
};

CuArgs::Argument<std::underlying_type_t<ImageDatabase::Decoder>> DisableDecoderArgument
{
	"--disable-decoders",
	CuStr::Format("disable decoders. {}", CuEnum::Strings<ImageDatabase::Decoder>()),
	0,
	[](const auto& v)
	{
		std::underlying_type_t<ImageDatabase::Decoder> buf = 0;
		CuStr::Split(v, ',', [&](const auto& vs)
			{
				buf |= CuUtil::ToUnderlying(CuEnum::TryFromString<ImageDatabase::Decoder>(vs));
			});
		return buf;
	},
	[](const auto v)
	{
		std::vector<std::string> buf{};
		for (const auto ev : CuEnum::Values<ImageDatabase::Decoder>())
		{
			if (CuUtil::ToUnderlying(ev) & v)
			{
				buf.emplace_back(CuEnum::ToString(ev));
			}
		}
		return CuStr::Join(buf.begin(), buf.end(), ", ");
	}
};

#pragma endregion ArgsDefs

namespace nlohmann
{
	template <>
	struct adl_serializer<ImageDatabase::ImageInfo>
	{
		static void to_json(json& j, const ImageDatabase::ImageInfo& v)
		{
			j = {
				{"path", std::string_view(reinterpret_cast<const char*>(v.Path.data()), v.Path.size())},
				{"md5", ImageDatabase::ImageInfo::RawMd5(v.Md5)},
				{"vgg16", std::span<float, 512>(const_cast<float*>(v.Vgg16.data()), size_t{512})}
			};
		}

		static void from_json(const json& j, ImageDatabase::ImageInfo& v)
		{
			throw ID_MakeExcept("not impl");
		}
	};
}

#pragma region Build
struct BuildParam
{
	std::filesystem::path DbPath;
	ImageDatabase::Device Dev;
	std::vector<std::filesystem::path> BuildPath;
	std::unordered_set<std::u8string> ExtBlackList;
	std::unordered_map<std::u8string, ImageDatabase::Decoder> ExtDecoderList;
	size_t ThreadNum;
	std::unordered_set<std::u8string> ZipList;
	bool SyncMode;
	std::underlying_type_t<ImageDatabase::Decoder> DisableDecoder;
};

std::filesystem::path BuildGetPath(const ImageDatabase::Reader::LoadType& t)
{
	return std::visit(CuUtil::Variant::Visitor{
		                  [](const ImageDatabase::Reader::PathType& info)
		                  {
			                  return info;
		                  },
		                  [](const ImageDatabase::Reader::MemoryType& info)
		                  {
			                  return info.Path;
		                  }
	                  }, t);
}

template <bool Mt = false, typename Cont>
void BuildCore(ImageDatabase::Database<Cont>& db, const BuildParam &param)
{
	if constexpr (Mt)
	{
		CuThread::Channel<std::optional<ImageDatabase::Reader::LoadType>, CuThread::Dynamics> mq{};
		mq.DynLimit = param.ThreadNum;
		CuThread::Synchronize push([&](ImageDatabase::PathType && p, ImageDatabase::Md5Type && m, ImageDatabase::Vgg16Type && v)
								   { db.Append(std::move(p), std::move(m), std::move(v)); });

		ImageDatabase::Reader reader([&](ImageDatabase::Reader& self, ImageDatabase::Reader::LoadType &&data)
									 { mq.Write(std::move(data)); },
									 param.BuildPath);
		reader.ExtBlackList = param.ExtBlackList;
		reader.ExtDecoderList = param.ExtDecoderList;
		reader.ZipList = param.ZipList;
		reader.DisableDecoder = param.DisableDecoder;

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

					LogInfo("scan: {}", BuildGetPath(*i));
					reader.Process([&](ImageDatabase::PathType&& p, ImageDatabase::Md5Type&& m, ImageDatabase::Vgg16Type&& v)
					{
						push(std::move(p), std::move(m), std::move(v));
					}, extractor, *i);
				}
			});
		}

		reader.Read();

		for (auto &_ : threads)
			mq.Write({});
		for (auto &t : threads)
			t.join();
	}
	else
	{
		ImageDatabase::Extractor extractor(param.Dev);
		ImageDatabase::Reader reader([&](ImageDatabase::Reader& self, ImageDatabase::Reader::LoadType&& data)
		                             {
			                             LogInfo("scan: {}", BuildGetPath(data));
			                             self.Process([&](ImageDatabase::PathType&& p, ImageDatabase::Md5Type&& m, ImageDatabase::Vgg16Type&& v)
			                             {
				                             db.Append(std::move(p), std::move(m), std::move(v));
			                             }, extractor, data);
		                             },
		                             param.BuildPath);
		reader.ExtBlackList = param.ExtBlackList;
		reader.ExtDecoderList = param.ExtDecoderList;
		reader.ZipList = param.ZipList;
		reader.DisableDecoder = param.DisableDecoder;

		reader.Read();
	}

	LogInfo("save database : {}", param.DbPath);
	db.Save(param.DbPath);
	LogInfo("database size: {}", db.Size());
}

template <bool Mt = false>
void BuildLaunch(const CuArgs::Arguments& args, const BuildParam& param)
{
	if (args.Value(NoCheckArg))
	{
		ImageDatabase::Database<ImageDatabase::VectorContainer> db{};
		const auto& databasePath = param.DbPath;
		CuAssert(!exists(databasePath));
		BuildCore<Mt>(db, param);
	}
	else
	{
		ImageDatabase::Database db{};
		if (exists(param.DbPath))
			db.Load(param.DbPath);
		LogInfo("database size: {}", db.Images.Data.size());
		BuildCore<Mt>(db, param);
	}
}

void BuildOperator(const CuArgs::Arguments& args)
{
	const BuildParam param{
					 args.Value(DatabaseArg),
					 args.Value(DeviceArg),
					 args.Value(PathArg),
					 args.Value(IgnoreExtArg),
					 args.Value(DecoderSpecArg),
					 args.Value(ThreadLimitArg),
					 args.Value(ZipExtArg),
					 args.Value(SyncModeArg),
					 args.Value(DisableDecoderArgument)};
	if (param.ThreadNum == 1)
	{
		BuildLaunch<false>(args, param);
	}
	else
	{
		BuildLaunch<true>(args, param);
	}
}
#pragma endregion Build

template <typename Cont>
void ConcatDatabase(ImageDatabase::Database<>& out, ImageDatabase::Database<Cont>& db)
{
	db.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m, const ImageDatabase::Vgg16Type& v)
	{
		if (const auto pos = out.Images.Data.find(p); pos != out.Images.Data.end())
		{
			const auto& [om, ov] = pos->second;
			LogWarn("dup path: {}. replace {} => {}, {}, ... => {}, ...",
			        ImageDatabase::ImageInfo::PathU8String(pos->first),
			        ImageDatabase::ImageInfo::Md5String(om), ImageDatabase::ImageInfo::Md5String(m),
			        ov[0], v[0]);
			pos->second = std::make_tuple(m, v);
		}
		else
		{
			out.Images.Data[p] = std::make_tuple(m, v);
		}
	});
}

void ConcatOperator(const CuArgs::Arguments& args)
{
	const auto outDbPath = args.Value(DatabaseArg);
	const auto concatDbPaths = args.Value(PathArg);
	const auto lazy = args.Value(LazyArg);

	ImageDatabase::Database outDb{};
	if (std::filesystem::exists(outDbPath))
	{
		outDb.Load(outDbPath);
		LogInfo("load database: {}: {}", outDbPath, outDb.Images.Data.size());
	}

	for (const auto& concatDbPath : concatDbPaths)
	{
		if (lazy)
		{
			ImageDatabase::Database<ImageDatabase::LazyContainer> concatDb(concatDbPath);
			LogInfo("load database: {}: {}", concatDbPath, concatDb.Size());
			ConcatDatabase(outDb, concatDb);
		}
		else
		{
			ImageDatabase::Database<ImageDatabase::VectorContainer> concatDb(concatDbPath);
			LogInfo("load database: {}: {}", concatDbPath, concatDb.Size());
			ConcatDatabase(outDb, concatDb);
		}
	}

	outDb.Save(outDbPath);
	LogInfo("save database: {}: {}", outDbPath, outDb.Size());
}

struct ReplaceParams
{
	std::filesystem::path OutPath;
	std::regex ReExp;
	std::string ReplaceValue;
};

std::optional<ImageDatabase::PathType> ReplaceOne(const ReplaceParams& params, const ImageDatabase::PathType& path)
{
	const auto psv = ImageDatabase::ImageInfo::PathU8String(path);
	const auto p = CuStr::ToDirtyUtf8StringView(psv);

	auto reBeg = std::regex_iterator(p.begin(), p.end(), params.ReExp);
	if (const auto reEnd = decltype(reBeg){}; std::distance(reBeg, reEnd))
	{
		decltype(reBeg->position()) pos = 0;
		std::string buf{};
		for (auto it = reBeg; it != reEnd; ++it)
		{
			std::copy_n(p.begin() + pos, it->position() - pos, std::back_inserter(buf));
			it->format(std::back_inserter(buf), params.ReplaceValue);
			pos = it->position() + it->length();
		}
		std::copy_n(p.begin() + pos, p.length() - pos, std::back_inserter(buf));

		LogInfo(R"({} => {})", psv, CuStr::FromDirtyUtf8String(buf));

		ImageDatabase::PathType ret;
		const auto ns = buf.size();
		ret.reserve(ns);
		std::copy_n(reinterpret_cast<char8_t*>(buf.data()), ns, ret.data());
		return std::move(ret);
	}

	return {};
}

template <typename Cont>
void ReplaceCore(const ReplaceParams& params, ImageDatabase::Database<Cont>& db)
{
	LogInfo("database size: {}", db.Size());

	auto fs = ImageDatabase::Database<>::CreateDatabaseFile(params.OutPath);
	if (std::is_same_v<Cont, ImageDatabase::LazyContainer>)
	{
		db.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m, const ImageDatabase::Vgg16Type& v)
			{
				if (const auto np = ReplaceOne(params, p); np)
				{
					ImageDatabase::Database<>::SaveImage(fs, *np, m, v);
				}
			});
	}
	else
	{
		db.ParallelForEach([&](ImageDatabase::ImageInfo& info)
			{
				if (const auto np = ReplaceOne(params, info.Path); np)
				{
					info.Path = std::move(*np);
				}
			});
		db.Save(fs);
	}
}

void ReplaceOperator(const CuArgs::Arguments& args)
{
	const auto dbPath = args.Value(DatabaseArg);
	const auto lazy = args.Value(LazyArg);

	const ReplaceParams params{
		args.Value(OutputArg), std::regex(args.Value(ReplaceRegexArg), args.Value(ReplaceRegexFlagArg)),
		args.Value(ReplaceValueArg)
	};

	LogInfo("load database: {}", dbPath);
	if (lazy)
	{
		ImageDatabase::Database<ImageDatabase::LazyContainer> db(dbPath);
		ReplaceCore(params, db);
	}
	else
	{
		ImageDatabase::Database<ImageDatabase::VectorContainer> db(dbPath);
		ReplaceCore(params, db);
	}
}

struct ExportBaseParams
{
	std::string Output;
};

struct ExportJsonParams : ExportBaseParams
{
	const int Indent = -1;
	const char IndentChar = ' ';
	const bool EnsureAscii = false;
	const nlohmann::json::error_handler_t ErrorHandler = nlohmann::json::error_handler_t::strict;
};

struct ExportSQLiteParams : ExportBaseParams
{
	std::string Table;
	std::string Columns;
};

struct ExportPostgreSQLParams : ExportBaseParams
{
	std::string Database;
	std::string Table;
	std::string Columns;
	std::string User;
	std::string Password;
};

using ExportParams = std::variant<ExportBaseParams, ExportJsonParams, ExportSQLiteParams, ExportPostgreSQLParams>;

template <typename Cont>
void ExportAsJson(ImageDatabase::Database<Cont>& db, const ExportParams& rawParams)
{
	if constexpr (!std::is_same_v<Cont, ImageDatabase::VectorContainer>)
	{
		throw ID_MakeExcept("assert(VectorContainer)");
	}

	const auto params = std::get<ExportJsonParams>(rawParams);
	const auto data = nlohmann::json(db.Images.Data).dump(params.Indent, params.IndentChar, params.EnsureAscii, params.ErrorHandler);
	CuFile::WriteAllBytes(params.Output, reinterpret_cast<const uint8_t*>(data.data()), data.length());
}

template <typename Cont>
void ExportAsJsonLines(ImageDatabase::Database<Cont>& db, const ExportParams& rawParams)
{
	const auto params = std::get<ExportJsonParams>(rawParams);

	std::ofstream fs(params.Output, std::ios::out | std::ios::binary);
	db.Images.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m,
	                      const ImageDatabase::Vgg16Type& v)
	{
		const auto data = nlohmann::json{
			{"path", std::string_view(reinterpret_cast<const char*>(p.data()), p.size())},
			{"md5", ImageDatabase::ImageInfo::RawMd5(m)},
			{"vgg16", ImageDatabase::ImageInfo::Vgg16Span(v)}
		}.dump(params.Indent, params.IndentChar, params.EnsureAscii, params.ErrorHandler);

		fs.write(data.data(), data.length());
		fs.write("\n", 1);
	});

	fs.close();
}

#ifdef ID_HAS_LIBPQXX
namespace PqApi
{
	pqxx::connection Connection(const std::string& opt)
	{
		try
		{
			return std::move(pqxx::connection{ opt });
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::connection::connection", "throw {}", typeid(ex).name());
		}
	}

	void Prepare(pqxx::connection& conn, const pqxx::zview& id, const pqxx::zview& sql)
	{
		try
		{
			conn.prepare(id, sql);
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::connection::prepare", "throw {}", typeid(ex).name());
		}
	}

	std::unique_ptr<pqxx::work> Work(pqxx::connection& conn)
	{
		try
		{
			return std::make_unique<pqxx::work>(conn);
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::work::work", "throw {}", typeid(ex).name());
		}
	}

	template<typename... Args>
	void ExecPrepared(pqxx::work& trans, const pqxx::zview& id, Args&&...args)
	{
		try
		{
			trans.exec_prepared(id, std::forward<Args>(args)...);
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::work::exec_prepared", "throw {}", typeid(ex).name());
		}
	}

	void Commit(pqxx::work& trans)
	{
		try
		{
			trans.commit();
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::work::commit", "throw {}", typeid(ex).name());
		}
	}

	pqxx::stream_to RawTable(pqxx::work& tx, std::string_view table, std::string_view columns)
	{
		try
		{
			return pqxx::stream_to::raw_table(tx, table, columns);
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::stream_to::raw_table", "throw {}", typeid(ex).name());
		}
	}

	template<typename... Ts>
	void WriteValues(pqxx::stream_to& st, Ts&& ... ts)
	{
		try
		{
			st.write_values(std::forward<Ts>(ts)...);
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::stream_to::write_values", "throw {}", typeid(ex).name());
		}
	}

	void Complete(pqxx::stream_to& st)
	{
		try
		{
			st.complete();
		}
		catch (const std::exception& ex)
		{
			LogErr("{}", CuStr::FromDirtyUtf8String(ex.what()));
			throw ID_MakeApiExcept("pqxx::stream_to::complete", "throw {}", typeid(ex).name());
		}
	}
}

template <typename Cont>
void ExportAsPostgreSQL(ImageDatabase::Database<Cont>& db, const ExportParams& rawParams)
{
	const auto params = std::get<ExportPostgreSQLParams>(rawParams);

	const auto host = CuStr::Split(params.Output, ':');
	if (host.size() != 2) throw ID_MakeExcept(R"(require "-o hostaddr:port")");

	auto pqConn = PqApi::Connection(
		CuStr::Format("hostaddr={} port={} dbname={} user={} password={}",
			host[0], host[1], params.Database, params.User, params.Password));

	const auto trans = PqApi::Work(pqConn);
	auto st = PqApi::RawTable(*trans, params.Table, params.Columns);

	db.Images.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m, const ImageDatabase::Vgg16Type& v)
		{
			PqApi::WriteValues(st,
				CuStr::ToDirtyUtf8StringView(std::u8string_view(p.data(), p.size())),
				CuStr::Appends(R"(\x)", std::string_view(m.data(), m.size())),
				CuUtil::CopyN<std::array<float, 512>>(const_cast<float*>(v.data()), 512));
		});
	PqApi::Complete(st);
	PqApi::Commit(*trans);
	LogInfo("commit.");
}
#endif

template <typename Cont>
void ExportAsCSV(ImageDatabase::Database<Cont>& db, const ExportParams& rawParams)
{
	const auto [output] = std::get<ExportBaseParams>(rawParams);

	CuCSV::Writer writer(output);
	db.Images.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m, const ImageDatabase::Vgg16Type& v)
		{
			writer.WriteRow(
				CuStr::ToDirtyUtf8StringView(ImageDatabase::ImageInfo::PathU8String(p)),
				ImageDatabase::ImageInfo::Md5String(m),
				nlohmann::json(ImageDatabase::ImageInfo::Vgg16Span(v)).dump());
		});
	writer.Close();
}

#ifdef ID_HAS_SQLITE3
namespace LiteApi
{
	CuExcept_MakeException(SQLiteException, CuExcept, U8Exception);

#define LiteException(api, rc) SQLiteException(CuStr::FormatU8("[" #api "] error {}: {}", rc, std::u8string_view(reinterpret_cast<const char8_t*>(sqlite3_errstr(rc)))))
#define ThrowIfLiteException(api, rc, ifExpr)\
	if (ifExpr)\
	{\
		throw LiteException(api, rc);\
	}

	class Statement
	{
		std::shared_ptr<sqlite3> db;
		std::shared_ptr<sqlite3_stmt> stmt;

	public:
		Statement() = delete;
		~Statement() = default;
		Statement(const Statement&) = delete;
		Statement(Statement&&) = default;

		Statement& operator=(const Statement&) = delete;
		Statement& operator=(Statement&&) = default;

		Statement(std::shared_ptr<sqlite3> rawDb, const std::u8string_view& sql) : db(std::move(rawDb))
		{
			sqlite3_stmt* stmtPtr;
			const char* tail = nullptr;
			if (const auto rc = sqlite3_prepare_v2(db.get(), reinterpret_cast<const char*>(sql.data()),
			                                       static_cast<int>(sql.length()), &stmtPtr, &tail); rc != SQLITE_OK)
			{
				if (tail)
					LogErr("found error:> {}", sql.substr(reinterpret_cast<const char8_t*>(tail) - sql.data()));
				throw LiteException(sqlite3_prepare_v2, rc);
			}
			stmt = std::shared_ptr<sqlite3_stmt>(stmtPtr, [parent=db](sqlite3_stmt* ptr)
			{
				if (const auto rc = sqlite3_finalize(ptr); rc != SQLITE_OK)
					LogWarn("[sqlite3_finalize] {}",
				        std::u8string_view(reinterpret_cast<const char8_t*>(sqlite3_errstr(rc))));
				(void)parent;
			});
		}

		void Bind(const int place, const std::u8string_view& str) const
		{
			const auto rc = sqlite3_bind_text(stmt.get(), place, reinterpret_cast<const char*>(str.data()),
				static_cast<int>(str.length()), nullptr);
			ThrowIfLiteException(sqlite3_bind_text, rc, rc != SQLITE_OK);
		}

		template <size_t S>
		void Bind(const int place, const std::span<const std::uint8_t, S>& data) const
		{
			const auto rc = sqlite3_bind_blob(stmt.get(), place, data.data(), static_cast<int>(data.size()), nullptr);
			ThrowIfLiteException(sqlite3_bind_blob, rc, rc != SQLITE_OK);
		}

		void Step() const
		{
			const auto rc = sqlite3_step(stmt.get());
			ThrowIfLiteException(sqlite3_step, rc, !(rc == SQLITE_ROW || rc == SQLITE_DONE));
		}

		template <typename... Args>
		void Binds(Args&&...args) const
		{
			auto idx = 1;
			(BindsImpl(idx, std::forward<Args>(args)), ...);
		}

	private:
		template <typename Args>
		void BindsImpl(int& idx, Args&& args) const
		{
			Bind(idx++, std::forward<Args>(args));
		}
	};

	class SQLite
	{
		std::shared_ptr<sqlite3> db;

	public:
		SQLite() = default;
		~SQLite() = default;
		SQLite(const SQLite&) = delete;
		SQLite(SQLite&&) = default;

		SQLite& operator=(const SQLite&) = delete;
		SQLite& operator=(SQLite&&) = default;

		SQLite(const std::filesystem::path& filename)
		{
			Open(filename);
		}

		void Open(const std::filesystem::path& filename)
		{
			sqlite3* dbPtr;
			const auto rc = sqlite3_open(reinterpret_cast<const char*>(filename.u8string().c_str()), &dbPtr);
			ThrowIfLiteException(sqlite3_open, rc, rc != SQLITE_OK);

			db = std::shared_ptr<sqlite3>(dbPtr, [](auto* ptr)
				{
					if (const auto rc = sqlite3_close(ptr); rc != SQLITE_OK)
						LogWarn("[sqlite3_close] {}",
							std::u8string_view(reinterpret_cast<const char8_t*>(sqlite3_errstr(rc))));
				});
		}

		[[nodiscard]] Statement Prepare(const std::u8string_view& sql) const
		{
			return Statement(db, sql);
		}

		void Exec(const std::u8string_view& sql, int (*callback)(void*, int, char**, char**) = nullptr, void* param = nullptr) const
		{
			CuAssert(sql[sql.size()] == 0);
			char* err = nullptr;
			if (const auto rc = sqlite3_exec(db.get(), reinterpret_cast<const char*>(sql.data()), callback, param, &err); rc != SQLITE_OK)
			{
				LogErr("[sqlite3_exec] {}", std::u8string_view(reinterpret_cast<char8_t*>(err)));
				sqlite3_free(err);
				err = nullptr;
				throw LiteException(sqlite3_exec, rc);
			}
		}
	};

	class Transaction
	{
	public:
		enum class Behavior
		{
			Commit, Rollback
		};

	private:
		const SQLite& db;

		bool isWorking;
		Behavior behavior;

	public:
		Transaction() = delete;
		Transaction(const Transaction&) = delete;
		Transaction(Transaction&&) = delete;

		Transaction& operator=(const Transaction&) = delete;
		Transaction& operator=(Transaction&&) = delete;

		explicit Transaction(const SQLite& rawDb, const Behavior exitBehavior = Behavior::Commit) : db(rawDb), behavior(exitBehavior)
		{
			db.Exec(u8"BEGIN TRANSACTION;");
			isWorking = true;
		}

		void Commit()
		{
			db.Exec(u8"END TRANSACTION;");
			isWorking = false;
		}

		void Rollback()
		{
			db.Exec(u8"ROLLBACK TRANSACTION;");
			isWorking = false;
		}

		~Transaction()
		{
			if (isWorking)
			{
				if (behavior == Behavior::Commit)
				{
					Commit();
				}
				else
				{
					Rollback();
				}
			}
		}
	};
}

template <typename Cont>
void ExportAsSQLite(ImageDatabase::Database<Cont>& db, const ExportParams& rawParams)
{
	const auto params = std::get<ExportSQLiteParams>(rawParams);

	const LiteApi::SQLite sqlDb(params.Output);

	const auto sql = CuStr::FormatU8("insert into {} ({}) values (?,?,?)", params.Table, params.Columns);
	LogInfo("=> {}", sql);
	const auto stmt = sqlDb.Prepare(sql);

	{
		LiteApi::Transaction trans(sqlDb, LiteApi::Transaction::Behavior::Rollback);
		db.Images.ForEach([&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m,
		                      const ImageDatabase::Vgg16Type& v)
		{
			stmt.Binds(ImageDatabase::ImageInfo::PathU8String(p),
			           std::span<const uint8_t, 16>(ImageDatabase::ImageInfo::RawMd5(m)),
			           CuStr::FromDirtyUtf8String(ImageDatabase::ImageInfo::Vgg16String(v)));

			stmt.Step();
		});
		trans.Commit();
	}
}
#endif

template <typename Cont>
void ExportCore(ImageDatabase::Database<Cont>& db, const ExportType exportType, const ExportParams& params)
{
	LogInfo("database size: {}", db.Images.Data.size());
	switch (exportType)
	{
	case ExportType::Json:
		ExportAsJson(db, params);
		break;
	case ExportType::JsonLines:
		ExportAsJsonLines(db, params);
		break;
#ifdef ID_HAS_LIBPQXX
	case ExportType::PostgreSQL:
		ExportAsPostgreSQL(db, params);
		break;
#endif
	case ExportType::CSV:
		ExportAsCSV(db, params);
		break;
#ifdef ID_HAS_SQLITE3
	case ExportType::SQLite:
		ExportAsSQLite(db, params);
		break;
#endif
	default:
		throw ID_MakeExcept("not impl");
	}
}

ExportParams GetExportParams(const ExportType exportType, const CuArgs::Arguments& args)
{
	switch (exportType)
	{
	case ExportType::Json:
	case ExportType::JsonLines:
		{
			ExportJsonParams params{};
			params.Output = args.Value(OutputArg);
			return params;
		}
	case ExportType::PostgreSQL:
		{
			ExportPostgreSQLParams params{
				args.Value(OutputDatabaseNameArg), args.Value(OutputTableNameArg), args.Value(OutputColumnsArg),
				args.Value(DatabaseUserArg), args.Value(DatabasePasswordArg)
			};
			params.Output = args.Value(OutputArg);
			return params;
		}
	case ExportType::CSV:
		return ExportBaseParams{ args.Value(OutputArg) };
	case ExportType::SQLite:
		{
		ExportSQLiteParams params;
		params.Table = args.Value(OutputTableNameArg);
			params.Columns = args.Value(OutputColumnsArg);
			params.Output = args.Value(OutputArg);
			return params;
		}
	default: throw ID_MakeExcept("not impl");
	}
}

void ExportOperator(const CuArgs::Arguments& args)
{
	const auto dbPath = args.Value(DatabaseArg);
	const auto exportType = args.Value(ExportTypeArg);
	const auto params = GetExportParams(exportType, args);

	LogInfo("load database: {}", dbPath);
	if (args.Value(LazyArg))
	{
		ImageDatabase::Database<ImageDatabase::LazyContainer> db(dbPath);
		ExportCore(db, exportType, params);
	}
	else
	{
		ImageDatabase::Database<ImageDatabase::VectorContainer> db(dbPath);
		ExportCore(db, exportType, params);
	}
}

struct QueryParams
{
	std::vector<std::filesystem::path> Input{};
	float Threshold{};
	ImageDatabase::Device Vgg16Device{};
	std::unordered_set<std::u8string> ExtBlackList{};
	std::unordered_map<std::u8string, ImageDatabase::Decoder> ExtDecoderList{};
	std::unordered_set<std::u8string> ZipList{};
};

template <typename Cont>
void QueryCore(ImageDatabase::Database<Cont>& db, const QueryParams& params)
{
	LogInfo("database size: {}", db.Size());

	LogInfo("load file: {}", params.Input);
	ImageDatabase::Extractor extractor(params.Vgg16Device);
	ImageDatabase::VectorContainer images{};
	ImageDatabase::Reader reader([&](ImageDatabase::Reader& self, ImageDatabase::Reader::LoadType&& data)
		{
			if (images.Data.empty())
			{
				self.Process([&]<typename T0, typename T1, typename T2>(T0 && p, T1 && m, T2 && v) {
					images.Append(
						std::forward<T0>(p), std::forward<T1>(m),
						std::forward<T2>(v));
				}, extractor, data);
			} },
		params.Input);
	reader.ExtBlackList = params.ExtBlackList;
	reader.ExtDecoderList = params.ExtDecoderList;
	reader.ZipList = params.ZipList;
	reader.Read();

	LogInfo("search start ...");
	const auto& img = images.Data[0];

	using CacheType = std::tuple<size_t, float>;
	std::vector<CacheType> cache(db.Size());
	CuUtil::Range<size_t> rng(db.Size());

	std::any param{};

	std::ifstream fs;
	if constexpr (std::is_same_v<Cont, ImageDatabase::LazyContainer>)
	{
		fs = db.Images.Open();
		param = &fs;
	}

	const auto createCache = [&](const size_t idx)
	{
		float val;
		db.Get(idx, [&](const ImageDatabase::PathType&, const ImageDatabase::Md5Type&,
			const ImageDatabase::Vgg16Type& v) { val = v.dot(img.Vgg16); }, param);
		return CacheType(idx, val);
	};

	if constexpr (std::is_same_v<Cont, ImageDatabase::LazyContainer>)
	{
		std::transform(rng.begin(), rng.end(), cache.begin(), createCache);
	}
	else
	{
		Parallel::Map(rng.begin(), rng.end(), cache.begin(), createCache);
	}

	Parallel::Sort(cache.begin(), cache.end(), [&](const auto& a, const auto& b)
		{ return std::greater()(std::get<1>(a), std::get<1>(b)); });

	LogInfo("search done.");
	for (const auto& [idx, val] : cache)
	{
		if (val >= params.Threshold)
		{
			db.Get(idx, [&](const ImageDatabase::PathType& p, const ImageDatabase::Md5Type& m,
				const ImageDatabase::Vgg16Type& v)
			{
					LogInfo("found {}: {}", nlohmann::json(CuStr::ToDirtyUtf8StringView(ImageDatabase::ImageInfo::PathU8String(p))).dump(), val);
				if (const auto p8 = std::filesystem::path(ImageDatabase::ImageInfo::PathU8String(p)); exists(p8))
				{
					std::string title{};
					try
					{
						title = p8.string();
					}
					catch (...)
					{
						title = CuStr::ToDirtyUtf8String(p8.u8string());
					}
					cv::imshow(title, CuImg::LoadFile_ImageRGB_OpenCV(p8).Raw());
					cv::waitKey();
				}
			}, param);
		}
		else
		{
			break;
		}
	}
}

void QueryOperator(const CuArgs::Arguments& args)
{
	const auto dbPath = args.Value(DatabaseArg);

	QueryParams params{};
	params.Input = args.Value(PathArg);
	params.Threshold = args.Value(ThresholdArg);
	params.Vgg16Device = args.Value(DeviceArg);
	params.ExtBlackList = args.Value(IgnoreExtArg);
	params.ExtDecoderList = args.Value(DecoderSpecArg);
	params.ZipList = args.Value(ZipExtArg);

	LogInfo("load database: {}", dbPath);
	if (args.Value(LazyArg))
	{
		ImageDatabase::Database<ImageDatabase::LazyContainer> db(dbPath);
		QueryCore(db, params);
	}
	else
	{
		ImageDatabase::Database<ImageDatabase::VectorContainer> db(dbPath);
		QueryCore(db, params);
	}
}

void AvLog(void* avc, int ffLevel, const char* fmt, va_list vl)
{
	auto outLv = CuLog::LogLevel::Info;
	if (ffLevel <= AV_LOG_ERROR) outLv = CuLog::LogLevel::Error;
	else if (ffLevel <= AV_LOG_WARNING) outLv = CuLog::LogLevel::Warn;
	else if (ffLevel <= AV_LOG_INFO) outLv = CuLog::LogLevel::Info;
	else if (ffLevel <= AV_LOG_TRACE) outLv = CuLog::LogLevel::Debug;

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
				LogErr("[av_log_format_line2] {}", CuVid::Detail::AV::AvStrError(ret));
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
		case CuLog::LogLevel::Error:
			LogErr("{}", bv);
			break;
		case CuLog::LogLevel::Warn:
			LogWarn("{}", bv);
			break;
		case CuLog::LogLevel::Verb:
			LogVerb("{}", bv);
			break;
		case CuLog::LogLevel::Debug:
			LogDebug("{}", bv);
			break;
		default:
			LogInfo("{}", bv);
			break;
		}
	}
}

int CvLog(const int status, const char* funcName, const char* errMsg,
	const char* fileName, const int line, void*)
{
	LogErr("[{}:{}] [{}] error {}: {}", fileName, line, funcName, status, errMsg);
	return 0;
}

void LogHandler(std::filesystem::path logFile)
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

	const std::unordered_map<CuLog::LogLevel, CuConsole::Color> colorMap
	{
		{CuLog::LogLevel::None, CuConsole::Color::White },
		{CuLog::LogLevel::Error, CuConsole::Color::Red },
		{CuLog::LogLevel::Warn, CuConsole::Color::Yellow },
		{CuLog::LogLevel::Info, CuConsole::Color::White },
		{CuLog::LogLevel::Verb, CuConsole::Color::Gray },
		{CuLog::LogLevel::Debug, CuConsole::Color::Blue }
	};

	while (true)
	{
		const auto [level, raw] = ImageDatabase::Log.Chan.Read();
		const auto& [time, thId, src, msg] = raw;

		auto out = CuStr::FormatU8("[{}] [{}] [{}] {}{}", CuEnum::ToString(level), ImageDatabase::Detail::LogTime(time), thId, src, msg);
		if (out[out.length() - 1] == u8'\n') out.erase(out.length() - 1);

		SetForegroundColor(colorMap.at(level));
		CuConsole::WriteLine(CuStr::ToDirtyUtf8StringView(out));

		if (!logFile.empty())
		{
			fs << CuStr::ToDirtyUtf8StringView(out) << std::endl;
			fs.flush();
		}

		if (level == CuLog::LogLevel::None)
		{
			fs.close();
			break;
		}
	}
}

int main(const int argc, const char *argv[])
{
	CuConsole::WriteLine("ImageDatabase version \"" __DATE__ " " __TIME__ "\" Copyright (c) 2020-2023 iriszero");
	CuConsole::WriteLine("  built with " BOOST_PLATFORM "/" BOOST_COMPILER);

#pragma region ArgsParse
	auto args = CuArgs::Arguments{};
	args.Add(OperatorArg, DatabaseArg, PathArg, ThresholdArg, ThreadLimitArg, DeviceArg, IgnoreExtArg, ZipExtArg,
	         DecoderSpecArg, LogLevelArg, LogFileArg, ReplaceRegexArg, ReplaceRegexFlagArg, ReplaceValueArg,
	         ExportTypeArg, LazyArg, OutputArg, OutputDatabaseNameArg, OutputTableNameArg, OutputColumnsArg,
	         DatabaseUserArg, DatabasePasswordArg, SyncModeArg, NoCheckArg, DisableDecoderArgument);

	const auto usage = [&]
	{ return CuStr::Format("Usage:\n{}\n{}", argv[0], args.GetDesc()); };

	try
	{
		args.Parse(argc, argv);
	}
	catch (const std::exception &ex)
	{
		CuConsole::SetForegroundColor(CuConsole::Color::Red);
		CuConsole::Error::WriteLine(ex.what());
		CuConsole::Error::WriteLine(usage());
		exit(EXIT_FAILURE);
	}
#pragma endregion ArgsParse

#pragma region SetLog
	static std::thread logThread;
	const auto logLevel = args.Value(LogLevelArg);
	ImageDatabase::Log.Level = logLevel;
	LogInfo("\n{}", args.GetValuesDesc());

	av_log_set_callback(AvLog);
	cv::redirectError(CvLog);

	logThread = std::thread(LogHandler, args.Value(LogFileArg));
#pragma endregion SetLog

#ifndef _MSC_VER
	struct sigaction sigIntHandler;

	sigIntHandler.sa_handler = [](int)
	{
		LogNone("Ctrl-C");
		logThread.join();
		exit(1);
	};
	sigemptyset(&sigIntHandler.sa_mask);
	sigIntHandler.sa_flags = 0;

	sigaction(SIGINT, &sigIntHandler, NULL);
#endif

	try
	{
		const auto extBlackList = args.Value(IgnoreExtArg);
		const auto extDecoderList = args.Value(DecoderSpecArg);
		const auto zipList = args.Value(ZipExtArg);
		std::unordered_map<Operator, std::function<void(const CuArgs::Arguments&)>>{
			{Operator::Build, BuildOperator},
			{Operator::Concat, ConcatOperator},
			{Operator::Replace, ReplaceOperator},
			{Operator::Export, ExportOperator},
			{Operator::Query, QueryOperator}
		}[args.Value(OperatorArg)](args);
	}
	catch (const CuExcept::Exception& ex)
	{
		LogErr("{}", ex.ToString());
		LogInfo(usage());
	}
	catch (const CuExcept::U8Exception& ex)
	{
		LogErr("{}", ex.ToString());
		LogInfo(usage());
	}
	catch (const std::exception &ex)
	{
		LogErr("{}", ex.what());
		LogInfo(usage());
	}

	LogNone("{ok}.");

	logThread.join();
}
