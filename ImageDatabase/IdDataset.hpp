#pragma once

#include <fstream>

#include <nlohmann/json.hpp>

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

	using PathType = std::u8string_view;
	using Md5Type = std::span<std::uint8_t, 16>;
	using Vgg16Type = std::span<float, 512>;
	using OcrType = std::u8string_view;
	using BarcodeType = std::u8string_view;
	using OrbType = std::span<uint8_t>;
	using SiftType = std::span<float>;

	struct DataRow
	{
		PathType Path;
		Md5Type Md5;
		Vgg16Type Vgg16;
		OcrType Ocr;
		BarcodeType Barcode;
		OrbType Orb;
		SiftType Sift;
	};

#define Id_Data_Prop(prop, setType) \
	decltype(auto) Get##prop() { return static_cast<Impl*>(this)->Get##prop(); } \
	void Set##prop(setType v) { static_cast<Impl*>(this)->Set##prop(v); } \
	constexpr bool Has##prop() { return static_cast<Impl*>(this)->Has##prop(); }

#define Id_Data_Has_Prop(prop) constexpr bool Has##prop() { return true; }

	template <typename Impl>
	struct IData
	{
		IData() = default;

		Id_Data_Prop(Path, const PathType&);
		Id_Data_Prop(Md5, const Md5Type&);
		Id_Data_Prop(Vgg16, const Vgg16Type&);
		Id_Data_Prop(Ocr, const OcrType&);
		Id_Data_Prop(Barcode, const BarcodeType&);
		Id_Data_Prop(Orb, const OrbType&);
		Id_Data_Prop(Sift, const SiftType&);
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

		Id_Data_Has_Prop(Path);
		Id_Data_Has_Prop(Md5);
		Id_Data_Has_Prop(Vgg16);
		Id_Data_Has_Prop(Ocr);
		Id_Data_Has_Prop(Barcode);
		Id_Data_Has_Prop(Orb);
		Id_Data_Has_Prop(Sift);

		decltype(auto) GetPath()
		{
			return GetStr("path");
		}

		void SetPath(const PathType& v)
		{
			Data["path"] = JsonType::string_t(reinterpret_cast<const char*>(v.data()), v.length());
		}

		decltype(auto) GetMd5()
		{
			std::array<Md5Type::element_type, Md5Type::extent> data{};
			Data["md5"].get_to(data);
			return data;
		}

		void SetMd5(const Md5Type& v)
		{
			Data["md5"] = v;
		}

		decltype(auto) GetVgg16()
		{
			return GetStr("vgg16");
		}

		void SetVgg16(const Vgg16Type& v)
		{
			Data["vgg16"] = v;
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

		decltype(auto) GetOrb()
		{
			return Data["orb"].get<std::vector<uint8_t>>();
		}

		void SetOrb(const OrbType& v)
		{
			Data["orb"] = v;
		}

		decltype(auto) GetSift()
		{
			return Data["sift"].get<std::vector<float>>();
		}

		void SetSift(const SiftType& v)
		{
			Data["sift"] = v;
		}
	};

	struct BinaryData : IData<BinaryData>
	{
		StringPointer<char8_t> Path;
		std::array<uint8_t, 16> Md5;
		std::array<float, 512> Vgg16;
		StringPointer<char8_t> Ocr;
		StringPointer<char8_t> Barcode;
		Pointer<uint8_t> Orb;

		Id_Data_Has_Prop(Path);
		Id_Data_Has_Prop(Md5);
		Id_Data_Has_Prop(Vgg16);
		Id_Data_Has_Prop(Ocr);
		Id_Data_Has_Prop(Barcode);
		Id_Data_Has_Prop(Orb);

		static const std::string& Desc()
		{
			static const auto desc = []
			{
				std::ostringstream desc{};
#define Id_BinaryData_MakeDesc(var) sizeof(decltype(var)) << "=" << #var

				desc << sizeof(BinaryData) << "|"
					<< Id_BinaryData_MakeDesc(Path) << ","
					<< Id_BinaryData_MakeDesc(Md5) << ","
					<< Id_BinaryData_MakeDesc(Vgg16) << ","
					<< Id_BinaryData_MakeDesc(Ocr) << ","
					<< Id_BinaryData_MakeDesc(Barcode) << ","
					<< Id_BinaryData_MakeDesc(Orb);

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

		Md5Type GetMd5()
		{
			return Md5;
		}

		void SetMd5(const Md5Type& v)
		{
			std::ranges::copy(v, Md5.begin());
		}

		Vgg16Type GetVgg16()
		{
			return Vgg16;
		}

		void SetVgg16(const Vgg16Type& v)
		{
			std::ranges::copy(v, Vgg16.begin());
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

		OrbType GetOrb()
		{
			return Orb;
		}

		void SetOrb(const OrbType& v)
		{
			Orb = Orb.CopyFrom(v);
		}
	};

	template <typename Impl, typename OutputStream, typename ValueType>
	struct IDataset
	{
		decltype(auto) MakeData(const DataRow& row)
		{
			return static_cast<Impl*>(this)->Insert(row);
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

		void Insert(ValueType value)
		{
			static_cast<Impl*>(this)->Insert(std::move(value));
		}

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

		FileOutputStream(const std::filesystem::path& path) : Stream(path, std::ios::out | std::ios::binary)
		{
			if (!Stream) throw Id_MakeExcept("create stream error");
		}
	};

	struct BinaryOutputStream
	{
		std::ofstream DataStream;
		std::ofstream IndexStream;

		BinaryOutputStream(const std::filesystem::path& path) : DataStream(path, std::ios::out | std::ios::binary), IndexStream(path.parent_path() / (path.filename().u8string() + u8".index"), std::ios::out | std::ios::binary)
		{
			if (!DataStream) throw Id_MakeExcept("create stream error");
			if (!IndexStream) throw Id_MakeExcept("create stream error");

			CuFile::WriteAllText(path.parent_path() / (path.filename().u8string() + u8".desc"), BinaryData::Desc());
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
			data.SetMd5(row.Md5);
			data.SetVgg16(row.Vgg16);
			data.SetOcr(row.Ocr);
			data.SetBarcode(row.Barcode);
			data.SetOrb(row.Orb);
			data.SetSift(row.Sift);

			return std::move(data);
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
		}

		decltype(Data)::iterator begin()
		{
			return Data.begin();
		}

		decltype(Data)::iterator end()
		{
			return Data.end();
		}

		void Insert(ValueType value)
		{
			Data.push_back(std::move(value));
		}

		static FileOutputStream CreateOutputStream(const std::filesystem::path& path)
		{
			return { path };
		}
	};

	struct BinaryDataset : IDataset<BinaryDataset, BinaryOutputStream, BinaryData>
	{
		std::vector<BinaryData> Data{};

		using ValueType = BinaryData;

		static ValueType MakeData(const DataRow& row)
		{
			BinaryData data{};

			data.SetPath(row.Path);
			data.SetMd5(row.Md5);
			data.SetVgg16(row.Vgg16);
			data.SetOcr(row.Ocr);
			data.SetBarcode(row.Barcode);
			data.SetOrb(row.Orb);

			return data;
		}

		void Loads(const std::filesystem::path& path)
		{
			std::ifstream fs(path, std::ios::in | std::ios::binary);
			if (!fs) return;

			const auto indexPath = path.parent_path() / (path.filename().u8string() + u8".index");
			const auto descPath = path.parent_path() / (path.filename().u8string() + u8".desc");

			std::ifstream indexFs(indexPath, std::ios::in | std::ios::binary);
			if (!indexFs) return;

			const auto desc = CuFile::ReadAllText(descPath);
			if (desc != BinaryData::Desc())
			{
				LogErr("mismatch desc: {} <=> {}", desc, BinaryData::Desc());
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
			std::vector<uint64_t> indexes(indexSize);
			for (uint64_t i = 0; i < indexSize; ++i)
			{
				indexes[i] = indexDeserialize.Read<uint64_t>();
			}

			Serialization::Deserialize fsDeserialize(fs);
			for (uint64_t i = 0; i < indexSize; ++i)
			{
				fs.seekg(indexes[i]);

				auto p = fsDeserialize.Read<std::u8string>();
				auto md5 = fsDeserialize.ReadArray<uint8_t, 16>();
				auto vgg16 = fsDeserialize.ReadArray<float, 512>();
				auto ocr = fsDeserialize.Read<std::u8string>();
				auto barcode = fsDeserialize.Read<std::u8string>();
				auto orb = fsDeserialize.Read<std::vector<uint8_t>>();

				Insert(MakeData({ p, md5, vgg16, ocr, barcode, orb }));
			}

			Data.shrink_to_fit();
		}

		static void Dump(BinaryOutputStream& stream, ValueType& it)
		{
			auto pos = stream.DataStream.tellp();

			Serialization::Serialize serialize(stream.DataStream);
			serialize.Write(static_cast<std::u8string_view>(it.Path));
			serialize.WriteArray(static_cast<Md5Type>(it.Md5));
			serialize.WriteArray(static_cast<Vgg16Type>(it.Vgg16));
			serialize.Write(static_cast<std::u8string_view>(it.Ocr), static_cast<std::u8string_view>(it.Barcode));
			serialize.WriteArray(static_cast<OrbType>(it.Orb));
			stream.DataStream.flush();

			Serialization::Serialize indexSerialize(stream.IndexStream);
			indexSerialize.Write<uint64_t>(pos);
			stream.IndexStream.flush();
		}

		decltype(Data)::iterator begin()
		{
			return Data.begin();
		}

		decltype(Data)::iterator end()
		{
			return Data.end();
		}

		void Insert(ValueType value)
		{
			Data.push_back(std::move(value));
		}

		static BinaryOutputStream CreateOutputStream(const std::filesystem::path& path)
		{
			return { path };
		}
	};
}
