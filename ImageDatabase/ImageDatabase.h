#pragma once

#include <cstdint>
#include <filesystem>
#include <string_view>
#include <vector>
#include <array>
#include <numeric>
#include <unordered_set>
#include <filesystem>

#include <eigen3/Eigen/Eigen>
#include <zip.h>
#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/dnn.hpp>

extern "C" {
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/pixdesc.h>
#include <libswscale/swscale.h>
}

#include "Cryptography.h"
#include "String.h"
#include "File.h"
#include "Enumerable.h"
#include "Serialization.h"
#include "Bit.h"

namespace ImageDatabase
{
	namespace __Detail
	{
		class MemoryStream
		{
			public:
				MemoryStream() = delete;
				MemoryStream(uint8_t* data, const uint64_t size, bool keep = false): size(size), keep(keep)
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

				~MemoryStream()
				{
					if (keep) delete[] data;
				}

				uint64_t Size()
				{
					return size;
				}

				int Read(unsigned char *buf, int buf_size)
				{
					if (buf_size < 0) return buf_size;
					
					if (index + buf_size >= size)
					{
						const auto n = size - index;
						std::copy_n(data + index, n, buf);
						return n;
					}

					std::copy_n(data + index, buf_size, buf);
					return buf_size;
				}

				int64_t Seek(int64_t offset, int whence)
				{
					if (whence == 0) // set
					{
						if (offset < 0) return -1;
						index = offset;
					}
					else if (whence == 1) // cur
					{
						index += offset;
					}
					else if (whence == 2) // end
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

		class my_iocontext_private
		{
			static const auto BufferSize = 4096;
		private:
			my_iocontext_private(my_iocontext_private const &);
			my_iocontext_private& operator = (my_iocontext_private const &);

		public:
			my_iocontext_private(MemoryStream inputStream)
			: inputStream_(inputStream)
			, buffer_(static_cast<unsigned char*>(::av_malloc(BufferSize))) {
			ctx_ = avio_alloc_context(buffer_, BufferSize, 0, this,
				&my_iocontext_private::read, NULL, &my_iocontext_private::seek); 
			}

			~my_iocontext_private() { 
				av_free(ctx_);
				av_free(buffer_); 
			}

			void reset_inner_context() { ctx_ = NULL; buffer_ = NULL; }

			static int read(void *opaque, unsigned char *buf, int buf_size) {
				my_iocontext_private* h = static_cast<my_iocontext_private*>(opaque);
				return h->inputStream_.Read(buf, buf_size); 
			}

			static int64_t seek(void *opaque, int64_t offset, int whence) {
				my_iocontext_private* h = static_cast<my_iocontext_private*>(opaque);

				if (0x10000 == whence)
					return h->inputStream_.Size();

				return h->inputStream_.Seek(offset, whence); 
			}

			::AVIOContext *get_avio() { return ctx_; }

			private:
			MemoryStream inputStream_; // abstract stream interface, You can adapt it to TMemoryStream  
			unsigned char * buffer_;  
			::AVIOContext * ctx_;
		};

		std::string FFmpegToString(int err)
		{
			char buf[4096]{0};
			const auto ret = av_strerror(err, buf, 4096);
			if (ret < 0) return String::FormatStr("unknow error code {}", ret);
			return std::string(buf);
		}
	}

	class Exception: public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};

	struct Image
	{
		std::filesystem::path Path{};
		std::array<char, 32> Md5{};
		Eigen::Matrix<float, 512, 1> Vgg16;
		float _Dot;
	};

	struct ImageFile
	{
		std::filesystem::path Path{};
		std::vector<Image> Images{};

		ImageFile() = delete;

		ImageFile(const std::filesystem::path& path)
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
					struct zip_stat zs;
					if (zip_stat_index(za, i, 0, &zs) == 0)
					{
						if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
						{
							auto* const zf = zip_fopen_index(za, i, 0);
							if (const auto err = zip_get_error(za)->zip_err; zf == nullptr && err != ZIP_ER_OK)
								throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_fopen_index] {}", err));

							//const auto buf = std::make_unique<char[]>(zs.size);
							std::string buf(zs.size, 0);
							
							if (const auto ret = zip_fread(zf, &buf[0], zs.size); ret < 0)
							{
								zip_fclose(zf);
								throw Exception(String::FormatStr("[ImageDatabase::ImageFile::ImageFile] [zip_fread] {}", ret));
							}

							LoadFile(path / filename, std::string_view(buf.data(), zs.size));
							//Images.emplace_back(path / filename, buf);

							zip_fclose(zf);
						}
					}
				}
				zip_close(za);
			}
			else
			{
				LoadFile(path);
				//Images.emplace_back(path);
			}
		}

		void SetLog(const std::function<void(std::string)>& callback)
		{
			log = callback;
		};

	private:
		std::optional<std::function<void(std::string)>> log = std::nullopt;

		void LoadFile(const std::filesystem::path& path)
		{
			AVFormatContext* fmtCtx = nullptr;
			int ret = avformat_open_input(&fmtCtx, path.u8string().c_str(), nullptr, nullptr);
			if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadFile(1)] [avforamt_open_input] {}",  __Detail::FFmpegToString(ret)));

			LoadImage(path, fmtCtx);

			if (fmtCtx != nullptr) avformat_close_input(&fmtCtx);
		}

		void LoadFile(const std::filesystem::path& path, const std::string_view& data)
		{
			__Detail::MemoryStream ms((uint8_t*)data.data(), data.length(), false);

			__Detail::my_iocontext_private priv_ctx(ms);
			AVFormatContext* ctx = avformat_alloc_context();
			ctx->pb = priv_ctx.get_avio();

			int ret = avformat_open_input(&ctx, NULL, NULL, NULL);
			if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadFile(2)] [avforamt_open_input] {}",  ret));

			LoadImage(path, ctx);

			if (ctx != nullptr) avformat_close_input(&ctx);
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
				int ret;

				ret = avformat_find_stream_info(fmtCtx, nullptr);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avformat_find_stream_info] {}", ret));

				ret = av_find_best_stream(fmtCtx, AVMEDIA_TYPE_VIDEO, -1, -1, &codec, 0);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [av_find_best_stream] {}", ret));

				const int streamId = ret;
				auto codecParams = fmtCtx->streams[streamId]->codecpar;

				codecCtx = avcodec_alloc_context3(codec);
				if (!codecCtx) throw Exception("[ImageDatabase::ImageFile::LoadImage] [avcodec_alloc_context3] NULL");

				ret = avcodec_parameters_to_context(codecCtx, codecParams);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_parameters_to_context] {}", ret));

				ret = avcodec_open2(codecCtx, codec, nullptr);
				if (ret < 0) throw Exception(String::FormatStr("[ImageDatabase::ImageFile::LoadImage] [avcodec_open2] {}", ret));

				constexpr auto dstWidth = 224; //codecParams->width;
				constexpr auto dstHeight = 224; //codecParams->height;
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

				std::string frameBuf(av_image_get_buffer_size(dstPixFmt, dstWidth, dstHeight, dstWidth), 0);
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

						Images.push_back(ProcImage(path / *Convert::ToString(index++), frameBuf, frame->linesize[0]));
					}
					av_packet_unref(pkt);
				} while (!eof);
				av_packet_free(&pkt);
			}
			catch (...)
			{
				if (decFrame != nullptr) av_frame_free(&decFrame);
				if (frame != nullptr) av_frame_free(&frame);
				if (codecCtx != nullptr) avcodec_free_context(&codecCtx);
				if (swsCtx != nullptr) sws_freeContext(swsCtx);
				std::throw_with_nested(Exception("[ImageDatabase::ImageFile::LoadImage] load image failed"));
			}

			if (decFrame != nullptr) av_frame_free(&decFrame);
			if (frame != nullptr) av_frame_free(&frame);
			if (codecCtx != nullptr) avcodec_free_context(&codecCtx);
			if (swsCtx != nullptr) sws_freeContext(swsCtx);
		}

		Image ProcImage(const std::filesystem::path& path, const std::string& buf, const int linesize)
		{
			Image out{};
			out.Path = path;
			if (log.has_value()) (*log)(path.u8string());

			Cryptography::Md5 md5{};
			md5.Append((uint8_t*)buf.data(), buf.length());
			const std::string md5Str = md5.Digest();
			std::copy_n(md5Str.begin(), 32, out.Md5.begin());
			if (log.has_value()) (*log)(md5Str);

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
								"[ImageDatabase::ImageFile::ProcImage::LoadFile] [File::ReadToEnd] init '{}' fail",
								file)));
				}
			};

			static std::string ProtoTxt = LoadFile("vgg16-deploy.prototxt");
			static std::string CaffeModel = LoadFile("vgg16.caffemodel");

			static auto vgg16 = []()
			{
				auto vgg16 = cv::dnn::readNetFromCaffe(
					ProtoTxt.data(), ProtoTxt.length(),
					CaffeModel.data(), CaffeModel.length());
				vgg16.setPreferableBackend(cv::dnn::DNN_BACKEND_VKCOM);
				vgg16.setPreferableTarget(cv::dnn::DNN_TARGET_VULKAN);
				return vgg16;
			}();
			
			cv::Mat image(224, 224, CV_8UC3, (void*)buf.data(), linesize);

			vgg16.setInput(cv::dnn::blobFromImage(image));
			auto feat = vgg16.forward();
			feat = feat / norm(feat);

			for (int i = 0; i < 512; ++i)
			{
				out.Vgg16(i, 0) = feat.at<float>(0, i, 0);
			}
			if (log.has_value()) (*log)(*Convert::ToString(out.Vgg16(0, 0)));

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

		void Load(const std::filesystem::path& path)
		{
			std::ifstream fs(path, std::ios::in | std::ios::binary);
			if (!fs) throw Exception("[ImageDatabase::Database::Load]");
			const auto fsbuf = std::make_unique<char[]>(4096);
			fs.rdbuf()->pubsetbuf(fsbuf.get(), 4096);

			Serialization::Unserialize unser(fs);
			while (fs)
			{
				Image img{};

				const auto [p] = unser.Read<std::string>();
				if (fs.gcount() == 0) break;
				img.Path = std::filesystem::u8path(p);
				if (log.has_value()) (*log)(img.Path.u8string());

				fs.read(img.Md5.data(), 32);
				if (log.has_value()) (*log)(std::string(img.Md5.data(), 32));

				fs.read((char*)img.Vgg16.data(), sizeof(float) * 512);
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

		void Save(const std::filesystem::path& path)
		{
			std::filesystem::remove(path);
			
			std::ofstream fs(path, std::ios::out | std::ios::binary);
			if (!fs) throw std::runtime_error("load file: bad stream");
			const auto fsbuf = std::make_unique<char[]>(4096);
			fs.rdbuf()->pubsetbuf(fsbuf.get(), 4096);

			Serialization::Serialize ser(fs);
			for (auto img : Images)
			{
				ser.Write(img.Path.u8string());
				fs.write(img.Md5.data(), 32);
				fs.write((char*)img.Vgg16.data(), sizeof(float) * 512);
			}

			fs.close();
		}

		void SetLog(const std::function<void(std::string)>& callback)
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
