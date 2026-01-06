#pragma once

#include <fstream>

#include <nlohmann/json.hpp>
#include <utility>

#include "IdExcept.hpp"
#include "IdUtils.hpp"
#include "File/File.hpp"
#include "Image/Image.hpp"

#include "Serialization.hpp"

namespace ImageDatabase
{
	struct RawData
	{
		std::u8string Path;
		CuImg::ImageBGR_OpenCV Image;
	};

	using HashMd5Type = std::span<std::uint8_t, 16>;

	using FeatureVgg16Type = std::span<float, 512>;

	using DescOrbType = std::span<uint8_t>;
	using DescSiftType = std::span<float>;

	using PathType = std::u8string_view;
	using HashType = HashMd5Type;
	using FeatureType = FeatureVgg16Type;
	using OcrType = std::u8string_view;
	using BarcodeType = std::u8string_view;
	using DescType = DescOrbType;

	struct DataRow
	{
		PathType Path;
		HashType Hash;
		FeatureType Feature;
		OcrType Ocr;
		BarcodeType Barcode;
		DescType Desc;
	};

#define Id_Data_Prop(prop, setType) \
	decltype(auto) Get##prop() { return static_cast<Impl*>(this)->Get##prop(); } \
	void Set##prop(setType v) { static_cast<Impl*>(this)->Set##prop(v); } \
	constexpr bool Has##prop() { return static_cast<Impl*>(this)->Has##prop(); }

	template <typename Impl>
	struct IData
	{
		IData() = default;

		Id_Data_Prop(Path, const PathType&);
		Id_Data_Prop(Hash, const HashType&);
		Id_Data_Prop(Feature, const FeatureType&);
		Id_Data_Prop(Ocr, const OcrType&);
		Id_Data_Prop(Barcode, const BarcodeType&);
		Id_Data_Prop(Desc, const DescType&);
	};

	struct JsonLinesData : IData<JsonLinesData>
	{
		using JsonType = nlohmann::basic_json<>;

		JsonType Data{};

	private:
		decltype(auto) GetStr(const char* key)
		{
			const auto* ptr = Data[key].get_ptr<const JsonType::string_t*>();
			return std::u8string_view{ reinterpret_cast<const char8_t*>(ptr->data()), ptr->length() };
		}

	public:
		JsonLinesData() = default;
		explicit JsonLinesData(JsonType data) : Data(std::move(data)) {}

		decltype(auto) GetPath()
		{
			return GetStr("path");
		}

		void SetPath(const PathType& v)
		{
			Data["path"] = JsonType::string_t(reinterpret_cast<const char*>(v.data()), v.length());
		}

		decltype(auto) GetHash()
		{
			std::array<HashType::element_type, HashType::extent> data{};
			Data["hash"].get_to(data);
			return data;
		}

		void SetHash(const HashType& v)
		{
			Data["hash"] = v;
		}

		decltype(auto) GetFeature()
		{
			return GetStr("feature");
		}

		void SetFeature(const FeatureType& v)
		{
			Data["feature"] = v;
		}

		decltype(auto) GetOcr()
		{
			return GetStr("ocr");
		}

		void SetOcr(const OcrType& v)
		{
			Data["ocr"] = CuStr::ToDirtyUtf8String(v);
		}

		std::u8string GetBarcode()
		{
			return std::u8string(CuStr::FromDirtyUtf8String(Data["barcode"].dump()));
		}

		void SetBarcode(const BarcodeType& v)
		{
			Data["barcode"] = nlohmann::json::parse(CuStr::ToDirtyUtf8StringView(v));
		}

		decltype(auto) GetDesc()
		{
			return Data["desc"].get<std::vector<uint8_t>>();
		}

		void SetDesc(const DescType& v)
		{
			Data["desc"] = v;
		}
	};

	struct BinaryData : IData<BinaryData>
	{
		StringPointer<char8_t> Path;
		std::array<uint8_t, 16> Hash;
		std::array<float, 512> Feature;
		StringPointer<char8_t> Ocr;
		StringPointer<char8_t> Barcode;
		Pointer<uint8_t> Desc;

		static const std::string& ColDesc()
		{
			static const auto desc = []
			{
				std::ostringstream desc{};
#define Id_BinaryData_MakeColDesc(var) sizeof(decltype(var)) << "=" << #var

				desc << sizeof(BinaryData) << "|"
					<< Id_BinaryData_MakeColDesc(Path) << ","
					<< Id_BinaryData_MakeColDesc(Hash) << ","
					<< Id_BinaryData_MakeColDesc(Feature) << ","
					<< Id_BinaryData_MakeColDesc(Ocr) << ","
					<< Id_BinaryData_MakeColDesc(Barcode) << ","
					<< Id_BinaryData_MakeColDesc(Desc);

				return desc.str();
			}();

			return desc;
		}

		PathType GetPath()
		{
			return Path;
		}

		void SetPath(const PathType& v)
		{
			Path = decltype(Path)::CopyFrom(v);
		}

		HashType GetHash()
		{
			return Hash;
		}

		void SetHash(const HashType& v)
		{
			std::ranges::copy(v, Hash.begin());
		}

		FeatureType GetFeature()
		{
			return Feature;
		}

		void SetFeature(const FeatureType& v)
		{
			std::ranges::copy(v, Feature.begin());
		}

		OcrType GetOcr()
		{
			return Ocr;
		}

		void SetOcr(const OcrType& v)
		{
			Ocr = Ocr.CopyFrom(v);
		}

		BarcodeType GetBarcode()
		{
			return Barcode;
		}

		void SetBarcode(const BarcodeType& v)
		{
			Barcode = Barcode.CopyFrom(v);
		}

		DescType GetDesc()
		{
			return Desc;
		}

		void SetDesc(const DescType& v)
		{
			Desc = Desc.CopyFrom(v);
		}
	};

	template <typename Impl, typename OutputStream, typename ValueType>
	struct IDataset
	{
		static ValueType MakeData(const DataRow& row)
		{
			ValueType data{};

			data.SetPath(row.Path);
			data.SetHash(row.Hash);
			data.SetFeature(row.Feature);
			data.SetOcr(row.Ocr);
			data.SetBarcode(row.Barcode);
			data.SetDesc(row.Desc);

			return data;
		}

		void Loads(const std::filesystem::path& path)
		{
			static_cast<Impl*>(this)->Load(path);
		}

		void Dump(OutputStream& stream, ValueType& iterator)
		{
			static_cast<Impl*>(this)->Dump(stream, iterator);
		}

		void Dumps(const std::filesystem::path& path)
		{
			auto fs = CreateOutputStream(path);

			auto itEnd = end();
			for (auto it = begin(); it != itEnd; ++it)
			{
				Dump(fs, it);
			}
		}

		// void Insert(ValueType value)
		// {
		// 	static_cast<Impl*>(this)->Insert(std::move(value));
		// }

		decltype(auto) begin()
		{
			return static_cast<Impl*>(this)->begin();
		}

		decltype(auto) end()
		{
			return static_cast<Impl*>(this)->end();
		}

		OutputStream CreateOutputStream(const std::filesystem::path& path)
		{
			return static_cast<Impl*>(this)->CreateOutputStream(path);
		}
	};

	struct FileOutputStream
	{
		std::ofstream Stream;

		FileOutputStream(const std::filesystem::path& path) : Stream(path, std::ios::out | std::ios::binary | std::ios::app)
		{
			if (!Stream) throw Id_MakeExcept("create stream error");
		}
	};

	struct BinaryOutputStream
	{
		std::ofstream DataStream;
		std::ofstream IndexStream;

		BinaryOutputStream(const std::filesystem::path& path) : DataStream(path, std::ios::out | std::ios::binary | std::ios::app), IndexStream(path.parent_path() / (path.filename().u8string() + u8".index"), std::ios::out | std::ios::binary)
		{
			if (!DataStream) throw Id_MakeExcept("create stream error");
			if (!IndexStream) throw Id_MakeExcept("create stream error");

			CuFile::WriteAllText(path.parent_path() / (path.filename().u8string() + u8".desc"), BinaryData::ColDesc());
		}
	};

	struct JsonLinesDataset : IDataset<JsonLinesDataset, FileOutputStream, JsonLinesData>
	{
		std::vector<JsonLinesData> Data{};

		using ValueType = JsonLinesData;

		static ValueType MakeData(const DataRow& row)
		{
			JsonLinesData data{};

			data.SetPath(row.Path);
			data.SetHash(row.Hash);
			data.SetFeature(row.Feature);
			data.SetOcr(row.Ocr);
			data.SetBarcode(row.Barcode);
			data.SetDesc(row.Desc);

			return data;
		}

		void Loads(const std::filesystem::path& path)
		{
			std::ifstream fs(path);
			if (!fs) return;

			std::string line{};
			while (std::getline(fs, line, '\n'))
			{
				if (line.empty()) continue;
				Data.emplace_back(JsonLinesData::JsonType::parse(line));
			}

			fs.close();

			Data.shrink_to_fit();
		}

		static void Dump(FileOutputStream& stream, const ValueType& it)
		{
			stream.Stream << it.Data << '\n';
			stream.Stream.flush();
		}

		decltype(Data)::iterator begin()
		{
			return Data.begin();
		}

		decltype(Data)::iterator end()
		{
			return Data.end();
		}

		// void Insert(ValueType value)
		// {
		// 	Data.push_back(std::move(value));
		// }

		static FileOutputStream CreateOutputStream(const std::filesystem::path& path)
		{
			return { path };
		}
	};

	struct BinaryDataset : IDataset<BinaryDataset, BinaryOutputStream, BinaryData>
	{
		std::vector<uint64_t> Indexes{};
		std::filesystem::path Path{};

		using ValueType = BinaryData;

		class iterator
		{
			const std::filesystem::path* path_ = nullptr;
			std::vector<uint64_t>::iterator it_{};
			std::vector<uint64_t>::iterator end_{};
			std::unique_ptr<ValueType> current_{};

		public:
			using iterator_category = std::random_access_iterator_tag;
			using value_type = BinaryData;
			using difference_type = int64_t;
			using pointer = value_type *;
			using reference = value_type &;

			iterator() = default;
			iterator(const std::filesystem::path& path, std::vector<uint64_t>::iterator it, std::vector<uint64_t>::iterator end) : path_(&path), it_(std::move(it)), end_(std::move(end)) {}
			iterator(const iterator& it) {
				path_ = it.path_;
				it_ = it.it_;
			}

			iterator &operator++()
			{
				current_.reset();
				++it_;
				return *this;
			}
			iterator operator++(int)
			{
				iterator retVal = *this;
				++(*this);
				return retVal;
			}

			bool operator==(iterator other) const {
				return it_ == other.it_;
			}
			bool operator!=(iterator other) const {
				return !(*this == other);
			}
			bool operator<(iterator other) const {
				return it_ < other.it_;
			}

			reference operator*() {
				if (!current_) {
					std::ifstream fs(*path_, std::ios::binary | std::ios::in);
					fs.seekg(*it_);

					Serialization::Deserialize fsDeserialize(fs);
					auto path = fsDeserialize.Read<std::u8string>();
					auto hash = fsDeserialize.ReadArray<uint8_t, 16>();
					auto feat = fsDeserialize.ReadArray<float, 512>();
					auto ocr = fsDeserialize.Read<std::u8string>();
					auto barcode = fsDeserialize.Read<std::u8string>();
					auto desc = fsDeserialize.Read<std::vector<uint8_t>>();

					current_ = std::make_unique<ValueType>(MakeData({
						path, hash, feat, ocr, barcode, desc
					}));
				}

				return *current_;
			}
			difference_type operator-(iterator other) const {
				return it_ - other.it_;
			}
		};

		void Loads(const std::filesystem::path& path)
		{
			{
				std::ifstream fs(path, std::ios::in | std::ios::binary);
				if (!fs) return;
			}

			const auto indexPath = path.parent_path() / (path.filename().u8string() + u8".index");
			const auto descPath = path.parent_path() / (path.filename().u8string() + u8".desc");

			std::ifstream indexFs(indexPath, std::ios::in | std::ios::binary);
			if (!indexFs) return;

			if (const auto desc = CuFile::ReadAllText(descPath); desc != BinaryData::ColDesc())
			{
				LogErr("mismatch desc: {} <=> {}", desc, BinaryData::ColDesc());
				return;
			}

			const auto indexFileSize = file_size(indexPath);
			if (indexFileSize % sizeof(uint64_t) != 0)
			{
				LogErr("error index file size");
				return;
			}

			const auto indexSize = indexFileSize / sizeof(uint64_t);

			Serialization::Deserialize indexDeserialize(indexFs);
			Indexes.resize(indexSize);
			for (uint64_t i = 0; i < indexSize; ++i)
			{
				Indexes[i] = indexDeserialize.Read<uint64_t>();
			}
			Indexes.shrink_to_fit();

			Path = path;
		}

		static void Dump(BinaryOutputStream& stream, ValueType& it)
		{
			const auto pos = stream.DataStream.tellp();

			Serialization::Serialize serialize(stream.DataStream);
			serialize.Write(static_cast<std::u8string_view>(it.Path));
			serialize.WriteArray(static_cast<HashType>(it.Hash));
			serialize.WriteArray(static_cast<FeatureType>(it.Feature));
			serialize.Write(static_cast<std::u8string_view>(it.Ocr));
			serialize.Write(static_cast<std::u8string_view>(it.Barcode));
			serialize.WriteArray(static_cast<DescType>(it.Desc));
			stream.DataStream.flush();

			Serialization::Serialize indexSerialize(stream.IndexStream);
			indexSerialize.Write<uint64_t>(pos);
			stream.IndexStream.flush();
		}

		iterator begin()
		{
			return {Path, Indexes.begin(), Indexes.end()};
		}

		iterator end()
		{
			return {{}, Indexes.end(), Indexes.end()};
		}

		// void Insert(ValueType value)
		// {
		// 	Data.push_back(std::move(value));
		// }

		static BinaryOutputStream CreateOutputStream(const std::filesystem::path& path)
		{
			return { path };
		}
	};
}
