#pragma once

#include "IdLogging.hpp"
#include "IdUtils.hpp"
#include "IdDataset.hpp"
#include "IdGenerator.hpp"
#include "IdExtractor.hpp"

#include <regex>

namespace ImageDatabase
{
	CuEnum_MakeEnumDef(Operator, Build, Query);
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

	struct DatasetParams
	{
		std::vector<std::pair<std::filesystem::path, DatasetType>> DatasetFiles{};
	};

	struct BuildParams : InputParams, DatasetParams
	{
	};

	struct QueryParams : InputParams, DatasetParams
	{
	};

	class Database
	{
#pragma region Build
		enum class WriteMode { Sync, Async };

		template <WriteMode Mode, typename Dataset, typename Stream>
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

			dataset.Dump(fs, data);

			if constexpr (Mode == WriteMode::Async) fsMtx.unlock();
			LogVerb("insert data {}ms", timer.Elapse().count());
		}

		template <typename DatasetType>
		void BuildImpl(const BuildParams& params)
		{
			Timer timer{};

			std::filesystem::path datasetPath{};
			if (params.DatasetFiles.empty())
			{
				throw std::invalid_argument("No dataset files specified");
			}
			if (params.DatasetFiles.size() > 1)
			{
				LogWarn("Multiple dataset files specified, only the first one will be used");
			}
			datasetPath = params.DatasetFiles.at(0).first;

			DatasetType dataset;
			dataset.Loads(datasetPath);
			LogVerb("loads {}ms", timer.Elapse().count());

std::unordered_set<std::u8string> paths{};
			for (auto& data : dataset) {
				paths.emplace(data.GetPath());
			}

			auto fs = dataset.CreateOutputStream(datasetPath);
			Generator generator{params.ZipExtensions, params.Ignores, params.ExtDecoderList };

			const auto createExtractor = [&]()
			{
				auto extractor = std::make_unique<Extractor>();
				extractor->Feature.PreferableDevice = params.ExtractorDevice;
				// extractor->Ocr.Languages = params.Languages;
				extractor->Init();
				return extractor;
			};

			if (params.Thread == 1)
			{
				auto extractor = createExtractor();

				for (auto* rawData : generator.Scan(params.Input))
				{
					BuildImplProc<WriteMode::Sync>(dataset, fs, *extractor, *rawData);
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

							BuildImplProc<WriteMode::Async>(dataset, fs, *extractor, raw);
						}
					});
				}

				for (auto&& rawData : generator.Scan(params.Input)) queue.Write(std::move(*rawData));

				for (auto& _ : threads) queue.Write({});

				for (auto& t : threads) if (t.joinable()) t.join();
			}
		}

		template <typename DatasetType>
		void BuildImplCall(const BuildParams& params)
		{
			BuildImpl<DatasetType>(params);
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
			if (params.DatasetFiles.at(0).second == DatasetType::JsonLines) BuildImplCall<JsonLinesDataset>(params);
			else if (params.DatasetFiles.at(0).second == DatasetType::Binary) BuildImplCall<BinaryDataset>(params);
		}

		void Query(const QueryParams& params)
		{

		}
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Operator);
CuEnum_MakeEnumSpec(ImageDatabase, DatasetType);
