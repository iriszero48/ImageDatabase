#include <Arguments/Arguments.hpp>

#include "IdDatabase.hpp"

#include <boost/config.hpp>
#include <boost/version.hpp>
#include <ZXing/Version.h>
#ifdef ID_ENABLE_RAPID_OCR_NCNN
#include <version.h>
#endif

#ifdef CU_IMG_HAS_DIRECTXTEX
#include <DirectXTex.h>
#endif

int main(const int argc, const char* argv[])
{
	CuConsole::WriteLine("ImageDatabase version \"" __DATE__ " " __TIME__ "\" Copyright (c) 2020-2026 iriszero");
	CuConsole::WriteLine("  built with " BOOST_PLATFORM "/" BOOST_COMPILER);
	CuConsole::WriteLine("+ tesseract/", tesseract::TessBaseAPI::Version());
	CuConsole::WriteLine("+ ffmpeg/", LIBAVCODEC_IDENT "|" LIBAVFORMAT_IDENT "|" LIBAVDEVICE_IDENT "|" LIBAVUTIL_IDENT "|" LIBSWSCALE_IDENT);
	CuConsole::WriteLine("+ opencv/", CV_VERSION);
	CuConsole::WriteLine("+ eigen/", EIGEN_WORLD_VERSION, ".", EIGEN_MAJOR_VERSION, ".", EIGEN_MINOR_VERSION);
	CuConsole::WriteLine("+ libzip/", zip_libzip_version());
	CuConsole::WriteLine("+ boost/", BOOST_LIB_VERSION);
	CuConsole::WriteLine("+ nlohmann-json/", NLOHMANN_JSON_VERSION_MAJOR, ".", NLOHMANN_JSON_VERSION_MINOR, ".", NLOHMANN_JSON_VERSION_PATCH);
	CuConsole::WriteLine("+ ZXing/", ZXING_VERSION_STR);
	CuConsole::WriteLine("+ GraphicsMagick/", MagickLibVersionText);
	CuConsole::WriteLine("+ cpphttplib/", CPPHTTPLIB_VERSION);
#ifdef CU_IMG_HAS_DIRECTXTEX
	CuConsole::WriteLine("+ DirectXTex/", DIRECTX_TEX_VERSION);
#endif
#ifdef ID_ENABLE_RAPID_OCR_NCNN
	CuConsole::WriteLine("+ RapidOcrNcnn/" VERSION);
#endif

	namespace Id = ImageDatabase;

	CuArgs::Arguments args{};

	CuArgs::EnumArgument<Id::Operator> operatorArg("-op", "operator " + CuStr::Views::Join(CuEnum::Strings<Id::Operator>(), "|"));
	args.Add(operatorArg);

	CuArgs::EnumArgument logLevelArg("--log-level", "LogLevel " + CuStr::Views::Join(CuEnum::Strings<CuLog::LogLevel>(), "|"), CuLog::LogLevel::Info);
	args.Add(logLevelArg);

	CuArgs::Argument<std::vector<std::filesystem::path>> inputArg(
		"-i",
		"input",
		[](const auto& v)
		{
			return v
			| std::views::split(';')
			| std::views::transform([](const auto& x) { return std::string(x.begin(), x.end()); })
			| std::ranges::to<std::vector<std::filesystem::path>>();
		}, 
		[](const auto& v)
		{
			return v
			| std::views::transform([](const auto& x) { return x.string(); })
			| std::views::join_with(';')
			| std::ranges::to<std::string>();
		});
	CuArgs::Argument<ImageDatabase::Regex> ignoresArg("--ignore", "ignore", [](const auto& x) { return Id::Regex(x); }, [](const auto& x) { return x.RawString; });
	CuArgs::Argument decoderArg("--decoder", "decoder " + CuStr::Views::Join(CuEnum::Strings<Id::Decoder>(), "|"), 0x0111);
	CuArgs::EnumArgument deviceArg("--device", "device", ImageDatabase::Device::vulkan);
	CuArgs::Argument<uint32_t> threadArg("-t", "thread", 1);
	CuArgs::Argument<std::vector<std::u8string>> zipExtsArg(
		"-z",
		"zip ext",
		std::vector<std::u8string>{u8".7z", u8".zip", u8".tar"},
		[](const auto& v)
		{
			return v
				| std::views::split(';')
				| std::views::transform([](const auto& x) { return CuStr::ToU8String(std::string(x.begin(), x.end())); })
				| std::ranges::to<std::vector<std::u8string>>();
		},
		[](const auto& v)
		{
			return v
			| std::views::transform([](const auto& x) { return CuStr::ToString(x); })
			| std::views::join_with(';')
			| std::ranges::to<std::string>();
		});

	CuArgs::Argument<std::unordered_map<std::u8string, ImageDatabase::Decoder>> extDecoderArg(
		"--ext-decoders",
		"ext decoders",
		std::unordered_map<std::u8string, ImageDatabase::Decoder>{},
		[](const auto& v)
		{
			return v
				| std::views::split(';')
				| std::views::transform([](const auto& x)
					{
						auto pair = x
							| std::views::split('=')
							| std::views::transform([](const auto& s) { return std::string(s.begin(), s.end()); })
							| std::ranges::to<std::vector<std::string>>();
						if (pair.size() != 2) throw Id_MakeExcept("parse error");
						return std::make_pair(CuStr::ToU8String(pair[0]), CuEnum::FromString<ImageDatabase::Decoder>(pair[1]).value());
					})
				| std::ranges::to<std::unordered_map<std::u8string, ImageDatabase::Decoder>>();
		},
		[](const auto& v)
		{
			return v
				| std::views::transform([](const auto& x) { return CuStr::Format("{}={}", x.first, CuEnum::ToString(x.second)); })
				| std::views::join_with(';')
				| std::ranges::to<std::string>();
		});
	CuArgs::Argument<> langArg("--lang", "lang", "chi_sim+eng+chi_tra+jpn");
	args.Add(inputArg, ignoresArg, decoderArg, deviceArg, threadArg, zipExtsArg, extDecoderArg, langArg);

	CuArgs::Argument<std::vector<std::filesystem::path>> datasetsArg(
		"-d",
		"datasets",
		[](const auto& v)
		{
			return v
				| std::views::split(';')
				| std::views::transform([](const auto& x) { return std::filesystem::path(x.begin(), x.end()); })
				| std::ranges::to<std::vector<std::filesystem::path>>();
		},
		[](const auto& v)
		{
			return v
				| std::views::transform([](const auto& x) { return CuStr::ToString(x); })
				| std::views::join_with(';')
				| std::ranges::to<std::string>();
		});
	CuArgs::Argument<std::vector<ImageDatabase::DatasetType>> typesArg(
		"--types",
		"DatasetTypes",
		[](const auto& v)
		{
			return v
				| std::views::split(';')
				| std::views::transform([](const auto& x) { return CuEnum::FromString<ImageDatabase::DatasetType>(std::string(x.begin(), x.end())).value(); })
				| std::ranges::to<std::vector<ImageDatabase::DatasetType>>();
		},
		[](const auto& v)
		{
			return v
				| std::views::transform([](const auto& x) { return CuEnum::ToString(x); })
				| std::views::join_with(';')
				| std::ranges::to<std::string>();
		});
	args.Add(datasetsArg, typesArg);

#define NO_CATCH
#ifndef NO_CATCH
	try
#endif
	{
		args.Parse(argc, argv);

		Id::Database Db{};

		Db.InitLog({ args.Value(logLevelArg) });

		LogInfo("\n{}", args.GetValuesDesc());

		const auto datasets = args.Value(datasetsArg);
		auto types = args.Get(typesArg);

		if (!types) {
			types = std::vector(datasets.size(), ImageDatabase::DatasetType::Binary);
		}

		CuAssert(datasets.size() == types->size());

		std::vector<std::pair<std::filesystem::path, ImageDatabase::DatasetType>> datasetFiles{};
		for (size_t i = 0; i < datasets.size(); ++i)
		{
			datasetFiles.emplace_back(datasets[i], (*types)[i]);
		}

		switch (args.Value(operatorArg))
		{
		case ImageDatabase::Operator::Build:
			Db.Build({
				args.Value(inputArg),
				args.Get(ignoresArg),
				args.Value(decoderArg),
				args.Value(deviceArg),
				args.Value(threadArg),
				args.Value(zipExtsArg),
				args.Value(extDecoderArg),
				args.Value(langArg),
				datasetFiles
			});
			break;
		case ImageDatabase::Operator::Query:

		default:
			break;
		}
	}
#ifndef NO_CATCH
	catch (const std::exception& exception)
	{
		CuConsole::Error::WriteLine(exception.what());
	}
#endif
}
