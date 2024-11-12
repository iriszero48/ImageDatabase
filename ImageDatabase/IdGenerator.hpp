#pragma once

#include <regex>
#if __cpp_lib_generator
#include <generator>
#else
#include <experimental/generator>
#endif

#include <Enum/Enum.hpp>
#include <Video/Video.hpp>

#include <Image/File.hpp>

#define Id_Yields(expr) for (auto* _value : (expr)) co_yield _value;

namespace ImageDatabase
{
	CuEnum_MakeEnumDef(Decoder,
		FFmpeg = 0x0001,
		GraphicsMagick = 0x0010,
		DirectXTex = 0x0100);

	struct Generator
	{
		std::vector<std::u8string> ZipExtensions{};
		std::optional<Regex> Ignore{};
		std::unordered_map<std::u8string, Decoder> ExtDecoderList{};

#if __cpp_lib_generator
		using GeneratorType = std::generator<RawData*>;
#else
		using GeneratorType = std::experimental::generator<RawData*>;
#endif

		template <typename T>
		struct IDecoder
		{
			static Decoder Name()
			{
				return T::Name();
			}

			GeneratorType operator()(const std::filesystem::path& path, const std::span<const uint8_t>& data)
			{
				return static_cast<T&>(*this)(path, data);
			}
		};

		struct FfmpegDecoder : IDecoder<FfmpegDecoder>
		{
			static Decoder Name()
			{
				return Decoder::FFmpeg;
			}

			GeneratorType operator()(const std::filesystem::path& path, const std::span<const uint8_t>& data) const
			{
				CuVid::DecoderBGR decoder{};
				if (!data.empty())
				{
					decoder.Config.Input = data;
				}
				else
				{
					decoder.Config.Input = path;
				}

				CuThread::Channel<RawData> chan{};

				decoder.Config.VideoHandler = [&](const CuImg::ImageBGR_Ref& raw)
					{
						auto img = ConvertToImageBGR_OpenCV(raw);

#if LIBAVCODEC_VERSION_MAJOR >= 60
						const auto idx = decoder.GetVideoCodecContext()->frame_num - 1;
#else
						const auto idx = decoder.GetVideoCodecContext()->frame_number - 1;
#endif
						const auto pu8 = idx < 1 ? path : path / *CuConv::ToString(idx);
						chan.Emplace(pu8.u8string(), ConvertToImageBGR_OpenCV(raw));
					};

				decoder.LoadFile();
				decoder.FindStream();
				uint64_t passed = 0;
				while (!decoder.Eof())
				{
					try
					{
						if (auto res = decoder.Read(); res != CuVid::StreamTypeNone)
						{
							auto raw = chan.Read();
							passed++;
							co_yield &raw;
						}
					}
					catch (const CuExcept::U8Exception& ex)
					{
						LogErr("{}: {}", path, ex.what());
					}
					catch (const CuExcept::Exception& ex)
					{
						LogErr("{}: {}", path, ex.what());
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}", path, ex.what());
					}
				}
				if (passed == 0) throw Id_MakeExcept("{}", "nop");
			}
		};

		struct GraphicsMagickDecoder : IDecoder<GraphicsMagickDecoder>
		{
			static Decoder Name()
			{
				return Decoder::GraphicsMagick;
			}

			GeneratorType operator()(const std::filesystem::path& path, const std::span<const uint8_t>& data) const
			{
				std::string mgPath{};
				Magick::Blob mgData{};
				if (path.empty())
				{
					try
					{
						mgPath = path.string();
					}
					catch (...)
					{
						uint8_t* ptr;
						size_t size;
						CuFile::ReadAllBytesAsPtr(path, &ptr, size);
						mgData.updateNoCopy(ptr, size);
					}
				}
				else
				{
					mgData.update(data.data(), data.size_bytes());
				}

				std::vector<Magick::Image> raw{};
				if (!mgPath.empty())
				{
					readImages(&raw, mgPath);
				}
				else
				{
					readImages(&raw, mgData);
				}

				for (size_t i = 0; i < raw.size(); ++i)
				{
					CuImg::DiscreteImageRGBOpacity16_GraphicsMagick buf{};
					buf.Raw() = raw[i];

					RawData ret{ (i < 1 ? path : path / *CuConv::ToString(i)).u8string(), ConvertToImageBGR_OpenCV(buf) };
					co_yield &ret;
				}
			}
		};

		struct DirectXTexDecoder : IDecoder<DirectXTexDecoder>
		{
			static Decoder Name()
			{
				return Decoder::DirectXTex;
			}

			GeneratorType operator()(const std::filesystem::path& path, const std::span<const uint8_t>& data) const
			{
#ifdef CU_IMG_HAS_DIRECTXTEX
				std::filesystem::path dxPath;
				bool useTmp = false;
				if (path.empty())
				{
					dxPath = path;
				}
				else
				{
					dxPath = TmpDir() / CuStr::FormatU8("{}{}", CuStr::Combine(std::hex, std::this_thread::get_id()), path.extension());
					CuFile::WriteAllBytes(dxPath, data.data(), data.size_bytes());
					useTmp = true;
				}
				auto raw = CuImg::LoadFile_ImageRGBA_DirectXTex(dxPath);
				const auto count = raw.Raw().GetImageCount();
				for (size_t i = 0; i < count; ++i)
				{
					const auto& [width, height, format, rowPitch, slicePitch, pixels] = raw.Raw().GetImages()[i];

					RawData ret{ (i < 1 ? path : path / *CuConv::ToString(i)).u8string(), ConvertToImageBGR_OpenCV(CuImg::ConvertToConstRef_RGBA(pixels, width, height, rowPitch)) };
					co_yield &ret;
				}
				if (useTmp) std::filesystem::remove(dxPath);
#else
				throw Id_MakeExcept("{}", "not impl");
#endif
			}

		private:
			static std::filesystem::path TmpDir()
			{
				static auto tmpDir = []
				{
					const auto tmp = std::filesystem::temp_directory_path() / "ImageDatabase";
					if (!exists(tmp)) create_directory(tmp);
					return tmp;
				}();

				return tmpDir;
			};
		};

		[[nodiscard]] bool IsZip(const std::u8string_view& ext) const
		{
			const auto extLower = CuStr::ToLowerU8(ext);
			for (const auto& zExt : ZipExtensions)
			{
				if (zExt == extLower) return true;
			}
			return false;
		}

		GeneratorType ScanZipImpl(const std::filesystem::path& file, zip_t* za)
		{
			const auto entries = zip_get_num_entries(za, 0);
			if (entries < 0)
			{
				const auto* err = zip_get_error(za);
				const auto msg = Detail::LibzipErrStr(*err);
				zip_close(za);
				throw Id_MakeApiExcept("zip_get_num_entries", "{}", msg);
			}

			for (int i = 0; i < entries; ++i)
			{
				struct zip_stat zs {};
				if (zip_stat_index(za, i, 0, &zs) == 0)
				{
					if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
					{
						auto* const zf = zip_fopen_index(za, i, 0);
						if (zf == nullptr)
						{
							const auto err = zip_get_error(za);
							LogWarn("{}({}): [zip_fopen_index] {}", file, filename, Detail::LibzipErrStr(*err));
							continue;
						}

						std::vector<uint8_t> buf(zs.size);
						if (const auto ret = zip_fread(zf, buf.data(), zs.size); ret < 0)
						{
							const auto err = zip_get_error(za);
							LogWarn("{}({}): [zip_fread] return {}: {}", file, filename, ret, Detail::LibzipErrStr(*err));
							zip_fclose(zf);
							continue;
						}

						const auto subPath = file / filename;

						if (IsZip(std::filesystem::path(CuStr::FromDirtyUtf8String(filename)).extension().u8string()))
						{
							Id_Yields(ScanZip(subPath, buf));
						}
						else
						{
							Id_Yields(ScanImage(subPath, buf));
						}

						zip_fclose(zf);
					}
				}
				else
				{
					const auto err = zip_get_error(za);
					LogWarn("{}({}): [zip_stat_index] {}", file, i, Detail::LibzipErrStr(*err));
				}
			}
			zip_close(za);
		}

		GeneratorType ScanZip(const std::filesystem::path& file, const std::span<uint8_t>& data)
		{
			zip_error_t error;
			zip_source_t* src = zip_source_buffer_create(data.data(), data.size(), 0, &error);
			if (src == nullptr)
				throw Id_MakeApiExcept("zip_source_buffer_create", "{}", Detail::LibzipErrStr(error));

			zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &error);
			if (za == nullptr)
				throw Id_MakeApiExcept("zip_open_from_source", "{}", Detail::LibzipErrStr(error));

			Id_Yields(ScanZipImpl(file, za));
		}

		GeneratorType ScanZip(const std::filesystem::path& file)
		{
			zip_t* za;
			int err;
			if ((za = zip_open(reinterpret_cast<const char*>(file.u8string().c_str()), 0, &err)) == nullptr) {
				zip_error_t error;
				zip_error_init_with_code(&error, err);
				const std::string errStr = zip_error_strerror(&error);
				zip_error_fini(&error);

				throw Id_MakeApiExcept("zip_open", "cannot open zip archive: {}", errStr);
			}

			Id_Yields(ScanZipImpl(file, za));
		}

		static std::function<GeneratorType(const std::filesystem::path&, const std::span<const uint8_t>&)> CreateDecoder(const Decoder name)
		{
			switch (name)
			{
			case Decoder::FFmpeg:
				return FfmpegDecoder{};
			case Decoder::GraphicsMagick:
				return GraphicsMagickDecoder{};
			case Decoder::DirectXTex:
				return DirectXTexDecoder{};
			default:
				assert(false);
				return {};
			}
		}

		GeneratorType ScanImageImpl(const std::filesystem::path& path, const std::span<uint8_t>& data)
		{
			const auto ext = CuStr::ToLowerU8(path.extension().u8string());
			std::optional<Decoder> specDec{};
			if (const auto p = ExtDecoderList.find(ext); p != ExtDecoderList.end()) specDec = p->second;

			std::vector<std::pair<Decoder, std::function<GeneratorType(const std::filesystem::path&, const std::span<const uint8_t>&)>>> decoders{};

			if (specDec)
			{
				decoders.emplace_back(*specDec, CreateDecoder(*specDec));
			}
			else
			{
				decoders.emplace_back(Decoder::FFmpeg, FfmpegDecoder{});
				decoders.emplace_back(Decoder::GraphicsMagick, GraphicsMagickDecoder{});
				decoders.emplace_back(Decoder::DirectXTex, DirectXTexDecoder{});
			}
			for (auto& [name, decoder] : decoders)
			{
				LogVerb("using decoder: {}", CuEnum::ToString(name));

				try
				{
					Id_Yields(decoder(path, data));
					co_return;
				}
				catch (const CuExcept::Exception& ex)
				{
					LogErr("{}: {}: {}", CuEnum::ToString(name), path, ex.ToString());
				}
				catch (const CuExcept::U8Exception& ex)
				{
					LogErr("{}: {}: {}", CuEnum::ToString(name), path, ex.ToString());
				}
				catch (const std::exception& ex)
				{
					LogErr("{}: {}: {}", CuEnum::ToString(name), path, ex.what());
				}
			}
		}

		GeneratorType ScanImage(const std::filesystem::path& file)
		{
			Id_Yields(ScanImageImpl(file, {}));
		}

		GeneratorType ScanImage(const std::filesystem::path& file, const std::span<uint8_t>& data)
		{
			Id_Yields(ScanImageImpl(file, data));
		}

		GeneratorType ScanFile(const std::filesystem::path& file)
		{
			if (is_symlink(file)) co_return;

			const auto pathU8 = file.u8string();
			if (Ignore && std::regex_match(reinterpret_cast<const char*>(pathU8.c_str()), Ignore->Native)) co_return;

			if (IsZip(file.extension().u8string()))
			{
				Id_Yields(ScanZip(file));
				co_return;
			}

			Id_Yields(ScanImage(file));
		}

		GeneratorType ScanPath(const std::filesystem::path& path)
		{
			if (is_regular_file(path))
			{
				Id_Yields(ScanFile(path));
			}
			else if (is_directory(path))
			{
				for (std::error_code error; auto & entry : std::filesystem::recursive_directory_iterator(path, std::filesystem::directory_options::skip_permission_denied, error))
				{
					if (error != std::error_code{})
					{
						LogErr("scan {} error: {}", entry.path(), error.message());
						error.clear();
						continue;
					}

					if (entry.is_symlink())
					{
						LogWarn("scan {}: is_symlink", entry.path());
						continue;
					}

					if (entry.is_regular_file())
					{
						Id_Yields(ScanFile(entry.path()));
					}
				}
			}
		}

		GeneratorType Scan(const std::vector<std::filesystem::path>& root)
		{
			for (const auto& path : root)
			{
				for (auto* value : ScanPath(path))
				{
					LogInfo("-> {}", value->Path);
					co_yield value;
				}
			}
		}
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Decoder);