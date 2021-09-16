#pragma once

#include <cstdint>
#include <filesystem>
#include <utility>
#include <vector>
#include <array>
#include <unordered_set>

#include <eigen3/Eigen/Eigen>
#include <opencv2/core.hpp>
#include <opencv2/dnn.hpp>

extern "C"
{
#include <zip.h>
	
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libswscale/swscale.h>
}

#include "Cryptography.h"
#include "String.h"
#include "File.h"
#include "Serialization.h"
#include "Bit.h"
#include "Thread.h"

namespace ImageDatabase
{
	class Exception : public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};
	
	namespace __Detail
	{
		class MemoryStream
		{
			public:
				MemoryStream() = delete;
				MemoryStream(uint8_t* data, const uint64_t size, const bool keep = false): size(size), keep(keep)
				{
					if (keep)
					{
						this->data = new uint8_t[size];
						std::copy_n(data, size, this->data);
					}
					else
					{
						this->data = data;
					}
				}

				MemoryStream(const MemoryStream& ms)
				{
					this->size = ms.size;
					if (ms.keep)
					{
						this->data = new uint8_t[this->size];
						std::copy_n(ms.data, ms.size, this->data);
					}
					else
					{
						this->data = ms.data;
					}
					this->keep = ms.keep;
					this->index = ms.index;
				}

				MemoryStream(MemoryStream&& ms) noexcept
				{
					this->size = ms.size;
					this->data = ms.data;
					ms.data = nullptr;
					this->keep = ms.keep;
					this->index = ms.index;
				}

				MemoryStream& operator=(const MemoryStream& ms)
				{
					if (this == &ms) return *this;
					
					this->size = ms.size;
					if (ms.keep)
					{
						this->data = new uint8_t[this->size];
						std::copy_n(ms.data, ms.size, this->data);
					}
					else
					{
						this->data = ms.data;
					}
					this->keep = ms.keep;
					this->index = ms.index;
					return *this;
				}
			
				MemoryStream& operator=(MemoryStream&& ms) noexcept
				{
					if (this == &ms) return *this;
					
					this->size = ms.size;
					this->data = ms.data;
					ms.data = nullptr;
					this->keep = ms.keep;
					this->index = ms.index;
					return *this;
				}
			
				~MemoryStream()
				{
					if (keep) delete[] data;
				}

				[[nodiscard]] uint64_t Size() const
				{
					return size;
				}

				int Read(unsigned char *buf, const int bufSize)
				{
					if (bufSize < 0) return bufSize;
					if (index >= size)
						return AVERROR_EOF;
					
					if (index + bufSize >= size)
					{
						const auto n = size - index;
						std::copy_n(data + index, n, buf);
						index += n;
						return n;
					}

					std::copy_n(data + index, bufSize, buf);
					index += bufSize;
					return bufSize;
				}

				int64_t Seek(const int64_t offset, const int whence)
				{
					if (whence == SEEK_SET)
					{
						if (offset < 0) return -1;
						index = offset;
					}
					else if (whence == SEEK_CUR)
					{
						index += offset;
					}
					else if (whence == SEEK_END)
					{
						if (offset > 0) return -1;
						index = size + offset;
					}
					else
					{
						throw std::runtime_error("Seek fail");
					}
					return 0;
				}

			private:
				uint8_t* data;
				uint64_t size;
				bool keep;
				uint64_t index = 0;
		};

		class MemoryStreamIoContext
		{
			static const auto BufferSize = 4096;

		public:
			MemoryStreamIoContext(MemoryStreamIoContext const&) = delete;
			MemoryStreamIoContext& operator = (MemoryStreamIoContext const&) = delete;
			MemoryStreamIoContext(MemoryStreamIoContext&&) = delete;
			MemoryStreamIoContext& operator = (MemoryStreamIoContext&&) = delete;
			
			MemoryStreamIoContext(MemoryStream inputStream):
				inputStream(std::move(inputStream)),
				buffer(static_cast<unsigned char*>(av_malloc(BufferSize)))
			{
				if (buffer == nullptr)
					throw Exception("[ImageDatabase::__Detail::MemoryStreamIoContext::MemoryStream] [av_malloc] the buffer cannot be allocated");
				
				ctx = avio_alloc_context(buffer, BufferSize, 0, this,
					&MemoryStreamIoContext::Read, nullptr, &MemoryStreamIoContext::Seek);
				if (ctx == nullptr)
					throw Exception("[ImageDatabase::__Detail::MemoryStreamIoContext::MemoryStream] [avio_alloc_context] the AVIO cannot be allocated");
			}

			~MemoryStreamIoContext()
			{
				av_free(ctx); 
			}

			void ResetInnerContext()
			{
				ctx = nullptr;
				buffer = nullptr;
			}

			static int Read(void *opaque, unsigned char *buf, const int bufSize)
			{
				auto h = static_cast<MemoryStreamIoContext*>(opaque);
				return h->inputStream.Read(buf, bufSize); 
			}

			static int64_t Seek(void *opaque, const int64_t offset, const int whence) {
				auto h = static_cast<MemoryStreamIoContext*>(opaque);

				if (0x10000 == whence)
					return h->inputStream.Size();

				return h->inputStream.Seek(offset, whence); 
			}

			[[nodiscard]] AVIOContext *GetAvio() const
			{
				return ctx;
			}

			private:
			MemoryStream inputStream;
			unsigned char* buffer = nullptr;
			AVIOContext* ctx = nullptr;
		};

		std::string FFmpegToString(const int err)
		{
			char buf[4096]{0};
			if (const auto ret = av_strerror(err, buf, 4096); ret < 0)
				return String::FormatStr("unknow error code {}", ret);
			return std::string(buf);
		}
	}

	struct Image
	{
		std::filesystem::path Path{};
		std::array<char, 32> Md5{};
		Eigen::Matrix<float, 512, 1> Vgg16{};
		float Dot{0};
	};

	struct ImageFile
	{
		std::filesystem::path Path{};
		Thread::Channel<Image> MessageQueue{};
		std::unordered_set<uint64_t> WhiteList{};

		ImageFile() = delete;

		ImageFile(const std::filesystem::path& path) : Path(path) {}

		void Parse()
		{
			const auto ext = Path.extension().u8string();

			if (static const std::unordered_set<std::string> CompExt{ ".zip", ".7z", ".rar", ".gz", ".xz" }; CompExt.find(ext) != CompExt.end())
			{
				const auto zipFile = File::ReadToEnd(Path);

				std::string errLog;
				zip_error_t error;
				zip_source_t* src = zip_source_buffer_create(zipFile.data(), zipFile.length(), 0, &error);
				if (src == nullptr && error.zip_err != ZIP_ER_OK)
					throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_source_buffer_create] {}", error.str));

				zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &error);
				if (za == nullptr && error.zip_err != ZIP_ER_OK)
					throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_open_from_source] {}", error.zip_err));

				const auto entries = zip_get_num_entries(za, 0);
				if (entries < 0)
				{
					std::string msg = zip_get_error(za)->str;
					zip_close(za);
					throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_get_num_entries] {}", msg));
				}

				for (int i = 0; i < entries; ++i)
				{
					struct zip_stat zs {};
					if (zip_stat_index(za, i, 0, &zs) == 0)
					{
						if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
						{
							auto* const zf = zip_fopen_index(za, i, 0);
							if (const auto err = zip_get_error(za)->zip_err; zf == nullptr && err != ZIP_ER_OK)
								throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_fopen_index] {}", err));

							std::string buf(zs.size, 0);

							if (const auto ret = zip_fread(zf, &buf[0], zs.size); ret < 0)
							{
								zip_fclose(zf);
								throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_fread] {}", ret));
							}

							LoadFile(Path / filename, std::string_view(buf.data(), zs.size));

							zip_fclose(zf);
						}
					}
				}
				zip_close(za);
			}
			else
			{
				LoadFile(Path);
			}
		}

		void SetLog(void(*callback)(const std::string&))
		{
			log = callback;
		}

	private:
		void(*log)(const std::string&) = nullptr;

		void LoadFile(const std::filesystem::path& path)
		{
			AVFormatContext* fmtCtx = nullptr;
			if (const int ret = avformat_open_input(&fmtCtx, path.u8string().c_str(), nullptr, nullptr); ret < 0)
				throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadFile(1)] [avforamt_open_input] {}",  __Detail::FFmpegToString(ret)));

			LoadImage(path, fmtCtx);

			avformat_close_input(&fmtCtx);
		}

		void LoadFile(const std::filesystem::path& path, const std::string_view& data)
		{
			__Detail::MemoryStream ms((uint8_t*)data.data(), data.length(), false);

			__Detail::MemoryStreamIoContext privCtx(ms);
			AVFormatContext* ctx = avformat_alloc_context();
			ctx->pb = privCtx.GetAvio();

			if (int ret = avformat_open_input(&ctx, nullptr, nullptr, nullptr); ret < 0)
				throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadFile(2)] [avforamt_open_input] {}",  ret));

			LoadImage(path, ctx);

			avformat_close_input(&ctx);
		}
		
		void LoadImage(const std::filesystem::path& path, AVFormatContext* fmtCtx)
		{
			AVCodec* codec = nullptr;
			AVCodecContext* codecCtx = nullptr;
			AVFrame* frame = nullptr;
			AVFrame* decFrame = nullptr;
			SwsContext* swsCtx = nullptr;

			try
			{
				int ret = avformat_find_stream_info(fmtCtx, nullptr);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avformat_find_stream_info] {}", ret));

				ret = av_find_best_stream(fmtCtx, AVMEDIA_TYPE_VIDEO, -1, -1, &codec, 0);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [av_find_best_stream] {}", ret));

				const int streamId = ret;
				const auto codecParams = fmtCtx->streams[streamId]->codecpar;

				codecCtx = avcodec_alloc_context3(codec);
				if (!codecCtx) throw Exception("[ImageDatabase::ImageFile::LoadImage] [avcodec_alloc_context3] NULL");

				ret = avcodec_parameters_to_context(codecCtx, codecParams);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_parameters_to_context] {}", ret));

				ret = avcodec_open2(codecCtx, codec, nullptr);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_open2] {}", ret));

				constexpr auto dstWidth = 224;
				constexpr auto dstHeight = 224;
				constexpr AVPixelFormat dstPixFmt = AV_PIX_FMT_BGR24;

				if (codecCtx->pix_fmt != AV_PIX_FMT_NONE)
				{
					swsCtx = sws_getCachedContext(
						nullptr, codecParams->width, codecParams->height, codecCtx->pix_fmt,
						dstWidth, dstHeight, dstPixFmt, 0, nullptr, nullptr, nullptr);
					if (!swsCtx) throw Exception("[ImageDatabase::ImageFile::LoadImage] [sws_getCachedContext] NULL");
				}

				frame = av_frame_alloc();
				if (!frame) throw Exception("[ImageDatabase::ImageFile::LoadImage] [av_frame_alloc] NULL");

				const auto frameSize = av_image_get_buffer_size(dstPixFmt, dstWidth, dstHeight, dstWidth);
				if (frameSize < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [av_image_get_buffer_size] {}", frameSize));

				std::string frameBuf(frameSize, 0);
				ret = av_image_fill_arrays(
					frame->data, frame->linesize,
					reinterpret_cast<uint8_t*>(frameBuf.data()),
					dstPixFmt, dstWidth, dstHeight, dstWidth);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [av_image_fill_arrays] {}", ret));
										
				decFrame = av_frame_alloc();
				if (!decFrame) throw Exception("[ImageDatabase::ImageFile::LoadImage] [av_frame_alloc] NULL");

				bool eof = false;
				AVPacket* pkt = av_packet_alloc();
				if (!pkt) throw Exception("[ImageDatabase::ImageFile::LoadImage] [av_packet_alloc] NULL");

				uint64_t index = 0;

				do
				{
					if (!eof)
					{
						ret = av_read_frame(fmtCtx, pkt);
						if (ret < 0 && ret != AVERROR_EOF) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [av_read_frame] {}", ret));
						
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
					else
					{
						if (!WhiteList.empty() && WhiteList.find(index) == WhiteList.end())
						{
							++index;
							continue;
						}
						ret = avcodec_send_packet(codecCtx, pkt);
						if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_send_packet] {}", ret));
					}
					while (ret >= 0)
					{
						ret = avcodec_receive_frame(codecCtx, decFrame);
						if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF)
						{
							av_packet_unref(pkt);
							break;
						}
						if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_receive_frame] {}", ret));

						if (swsCtx == nullptr)
						{
							swsCtx = sws_getCachedContext(
								nullptr, codecParams->width, codecParams->height, codecCtx->pix_fmt,
								dstWidth, dstHeight, dstPixFmt, 0, nullptr, nullptr, nullptr);
							if (!swsCtx) throw Exception("[ImageDatabase::ImageFile::LoadImage] [sws_getCachedContext] NULL");
						}
						
						sws_scale(swsCtx, decFrame->data, decFrame->linesize, 0, decFrame->height, frame->data, frame->linesize);

						MessageQueue.Write(ProcImage(path / *Convert::ToString(index++), frameBuf, frame->linesize[0]));
					}
					av_packet_unref(pkt);
				} while (!eof);
				av_packet_free(&pkt);
			}
			catch (...)
			{
				av_frame_free(&decFrame);
				av_frame_free(&frame);
				avcodec_free_context(&codecCtx);
				sws_freeContext(swsCtx);
				MessageQueue.Write({});
				std::throw_with_nested(Exception("[ImageDatabase::ImageFile::LoadImage] load image failed"));
			}

			av_frame_free(&decFrame);
			av_frame_free(&frame);
			avcodec_free_context(&codecCtx);
			sws_freeContext(swsCtx);
			MessageQueue.Write({});
		}

		Image ProcImage(const std::filesystem::path& path, const std::string& buf, const int lineSize) const
		{
			Image out{};
			out.Path = path;
			if (log != nullptr)
				log(path.u8string());

			Cryptography::Md5 md5{};
			md5.Append((uint8_t*)buf.data(), buf.length());
			const std::string md5Str = md5.Digest();
			std::copy_n(md5Str.begin(), 32, out.Md5.begin());
			if (log != nullptr) log(md5Str);

			static const auto LoadFile = [](const char* file)
			{
				try
				{
					return File::ReadToEnd(file);
				}
				catch(...)
				{
					std::throw_with_nested(
						Exception(
							String::FormatStr(
								"[ImageDatabase::ImageFile::ProcImage::loadFile] [File::ReadToEnd] init '{}' fail",
								file)));
				}
			};

			static std::string protoTxt = LoadFile("vgg16-deploy.prototxt");
			static std::string caffeModel = LoadFile("vgg16.caffemodel");

			static auto vgg16 = []()
			{
				auto vgg16 = cv::dnn::readNetFromCaffe(
					protoTxt.data(), protoTxt.length(),
					caffeModel.data(), caffeModel.length());
				vgg16.setPreferableBackend(cv::dnn::DNN_BACKEND_DEFAULT);
				vgg16.setPreferableTarget(cv::dnn::DNN_TARGET_OPENCL);
				return vgg16;
			}();

			const cv::Mat image(224, 224, CV_8UC3, (void*)buf.data(), lineSize);
			//cv::imshow("", image);
			//cv::waitKey(0);
			vgg16.setInput(cv::dnn::blobFromImage(image));
			auto feat = vgg16.forward();
			feat = feat / norm(feat);

			for (int i = 0; i < 512; ++i)
			{
				out.Vgg16(i, 0) = feat.template at<float>(0, i, 0);
			}
			if (log != nullptr) log(*Convert::ToString(out.Vgg16(0, 0)));

			return out;
		}
	};

	class Database
	{
	public:
		Database() = default;

		Database(std::filesystem::path path) : path(std::move(path))
		{
			Load(this->path);
		}

		void Load(const std::filesystem::path& dbPath)
		{
			std::ifstream fs(dbPath, std::ios::in | std::ios::binary);
			if (!fs) throw Exception("[ImageDatabase::Database::Load]");
			const auto fsBuf = std::make_unique<char[]>(4096);
			fs.rdbuf()->pubsetbuf(fsBuf.get(), 4096);

			Serialization::Deserialize deser(fs);
			while (fs)
			{
				Image img{};

				std::string p;
				try
				{
					auto [raw] = deser.Read<std::string>();
					p = raw;
				}
				catch (Serialization::Eof)
				{
					break;
				}
				img.Path = std::filesystem::u8path(p);
				if (log.has_value()) (*log)(img.Path.u8string());

				fs.read(img.Md5.data(), 32);
				if (log.has_value()) (*log)(std::string(img.Md5.data(), 32));

				fs.read(reinterpret_cast<char*>(img.Vgg16.data()), sizeof(float) * 512);
				if constexpr (Bit::Endian::Native == Bit::Endian::Big)
				{
					auto ptr = img.Vgg16.data();
					for (auto i = 0; i < 512; ++i)
					{
						ptr[i] = Bit::EndianSwap(ptr[i]);
					}
				}
				if (log.has_value()) (*log)(*Convert::ToString(img.Vgg16.data()[0]));

				Images.push_back(img);
			}

			fs.close();
		}

		static void SaveImage(std::ofstream& fs, const Image& img)
		{
			Serialization::Serialize ser(fs);
			const auto& [path, md5, vgg16, Dot] = img;
			ser.Write(path.u8string());
			fs.write(md5.data(), 32);
			fs.write(reinterpret_cast<const char*>(vgg16.data()), sizeof(float) * 512);
		}

		void Save()
		{
			Save(path);
		}
		
		void Save(const std::filesystem::path& savePath)
		{
			std::filesystem::remove(savePath);
			
			std::ofstream fs(savePath, std::ios::out | std::ios::binary);
			if (!fs) throw std::runtime_error("load file: bad stream");
			const auto fsBuf = std::make_unique<char[]>(4096);
			fs.rdbuf()->pubsetbuf(fsBuf.get(), 4096);

			for (const auto& img : Images)
			{
				SaveImage(fs, img);
			}

			fs.close();
		}

		void SetLog(void(*callback)(const std::string&))
		{
			log = callback;
		}
	private:
		std::filesystem::path path;
		std::optional<std::function<void(std::string)>> log = std::nullopt;

	public:
		std::vector<Image> Images;
	};
}
