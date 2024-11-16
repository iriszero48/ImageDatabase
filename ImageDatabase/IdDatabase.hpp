#pragma once

#include "IdLogging.hpp"
#include "IdUtils.hpp"
#include "IdDataset.hpp"
#include "IdGenerator.hpp"
#include "IdExtractor.hpp"

#include <regex>

namespace ImageDatabase
{
	CuEnum_MakeEnumDef(Operator, Build);
	CuEnum_MakeEnumDef(DatasetType, JsonLines, Binary);

	struct LogParams
	{
		CuLog::LogLevel LogLevel;
	};

	struct InputParams
	{
		std::vector<std::filesystem::path> Input;
		std::optional<Regex> Ignores;
		std::underlying_type_t<Decoder> Decoders;
		Device ExtractorDevice;
		uint32_t Thread;
		std::vector<std::u8string> ZipExtensions;
		std::unordered_map<std::u8string, Decoder> ExtDecoderList;
		std::string Languages;
	};

	struct OutputDatasetParams
	{
		std::filesystem::path OutputDatasetPath;
		DatasetType OutputDatasetType;
		bool UseBuffer;
	};

	struct BuildParams : InputParams, OutputDatasetParams
	{
	};

	class Database
	{
#pragma region Build
		enum class WriteMode { Sync, Async };

		template <WriteMode Mode, bool UseBuffer, typename Dataset, typename Stream>
		static void BuildImplProc(Dataset& dataset, Stream& fs, Extractor& extractor, const RawData& raw)
		{
			LogInfo("build {}", CuStr::FromDirtyUtf8String(CuStr::Combine(std::quoted(CuStr::ToDirtyUtf8StringView(raw.Path)))));

			static std::mutex fsMtx{};

			Timer timer{};
			auto exData = extractor(raw);
			LogVerb("extractor {}ms", timer.Elapse().count());

			timer.Reset();
			auto data = dataset.MakeData(exData);
			LogVerb("construct data {}ms", timer.Elapse().count());

			timer.Reset();
			if constexpr (Mode == WriteMode::Async) fsMtx.lock();

			if constexpr (!UseBuffer)
			{
				dataset.Dump(fs, data);
			}
			else
			{
				dataset.Insert(std::move(data));
			}

			if constexpr (Mode == WriteMode::Async) fsMtx.unlock();
			LogVerb("insert data {}ms", timer.Elapse().count());
		}

		template <typename DatasetType, bool UseBuffer>
		void BuildImpl(const BuildParams& params)
		{
			Timer timer{};

			DatasetType dataset;
			dataset.Loads(params.OutputDatasetPath);
			LogVerb("loads {}ms", timer.Elapse().count());

			auto fs = dataset.CreateOutputStream(params.OutputDatasetPath);
			Generator generator{params.ZipExtensions, params.Ignores, params.ExtDecoderList };

			const auto createExtractor = [&]()
			{
				auto extractor = std::make_unique<Extractor>();
				extractor->Vgg16.PreferableDevice = params.ExtractorDevice;
				extractor->Ocr.Languages = params.Languages;
				extractor->Init();
				return extractor;
			};

			if constexpr (!UseBuffer)
			{
				for (auto& data : dataset) dataset.Dump(fs, data);
			}

			if (params.Thread == 1)
			{
				auto extractor = createExtractor();

				for (auto* rawData : generator.Scan(params.Input))
				{
					BuildImplProc<WriteMode::Sync, UseBuffer>(dataset, fs, *extractor, *rawData);
				}
			}
			else
			{
				const auto threadNum = params.Thread ? params.Thread : std::thread::hardware_concurrency();
				CuThread::Channel<RawData> queue{};
				queue.DynLimit = threadNum;
				
				std::vector<std::thread> threads{};
				for (uint32_t i = 0; i < threadNum; ++i)
				{
					threads.emplace_back([&]
					{
						auto extractor = createExtractor();

						while (true)
						{
							auto raw = queue.Read();
							if (raw.Path.empty()) break;

							BuildImplProc<WriteMode::Async, UseBuffer>(dataset, fs, *extractor, raw);
						}
					});
				}

				for (auto&& rawData : generator.Scan(params.Input)) queue.Write(std::move(*rawData));

				for (auto& _ : threads) queue.Write({});

				for (auto& t : threads) if (t.joinable()) t.join();
			}

			if constexpr (UseBuffer)
			{
				timer.Reset();
				for (auto& data : dataset) dataset.Dump(fs, data);
				LogVerb("save {}ms", timer.Elapse().count());
			}
		}

		template <typename DatasetType>
		void BuildImplCall(const BuildParams& params)
		{
			if (params.UseBuffer) BuildImpl<DatasetType, true>(params);
			else BuildImpl<DatasetType, false>(params);
		}
#pragma endregion

		std::thread logThread_;

	public:
		Database()
		{
			
		}

		~Database()
		{
			LogNone("done.");

			if (logThread_.joinable()) logThread_.join();
		}

		void InitLog(const LogParams& params)
		{
			Log.Level = params.LogLevel;

			logThread_ = std::thread(LogHandler);
		}

		void Build(const BuildParams& params)
		{
			if (params.OutputDatasetType == DatasetType::JsonLines) BuildImplCall<JsonLinesDataset>(params);
			else if (params.OutputDatasetType == DatasetType::Binary) BuildImplCall<BinaryDataset>(params);
		}
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Operator);
CuEnum_MakeEnumSpec(ImageDatabase, DatasetType);
