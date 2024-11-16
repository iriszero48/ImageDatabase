#pragma once

#include <Enum/Enum.hpp>
#include <File/File.hpp>
#include <Cryptography/Md5.hpp>
#include <Image/Image.hpp>

#include <opencv2/opencv.hpp>
#include <Eigen/Eigen>
#include <tesseract/baseapi.h>
#include <ZXing/ReadBarcode.h>

#include "IdExcept.hpp"
#include "IdDataset.hpp"
#include "IdLogging.hpp"
#include "IdUtils.hpp"

namespace ImageDatabase
{
	// ReSharper disable CppInconsistentNaming
	CuEnum_MakeEnumDef(Device, cpu, cuda, opencl);
	// ReSharper restore CppInconsistentNaming

	template <typename T>
	struct IExtractor
	{
		void Init()
		{
			static_cast<T*>(this)->Init();
		}

		decltype(auto) operator()(const RawData& data)
		{
			return static_cast<T&>(*this)(data);
		}
	};

	struct Md5Extractor : IExtractor<Md5Extractor>
	{
		static void Init() {}

		std::array<uint8_t, 16> operator()(const RawData& data) const
		{
			CuCrypto::Md5 md5;
			const auto linesize = data.Image.Linesize();
			const auto usedSize = CuImg::CuBGR::ColorSize() * data.Image.Width();
			const auto height = data.Image.Height();
			if (linesize == usedSize)
			{
				md5.Append(data.Image.Data(), data.Image.Size());
			}
			else
			{
				for (size_t i = 0; i < height; ++i)
				{
					md5.Append(data.Image.Data() + linesize * i, usedSize);
				}
			}
			auto digest = md5.Digest();
			LogVerb("md5: {}", digest.ToString());

			return std::move(digest.Data);
		}
	};

	struct Vgg16Extractor : IExtractor<Vgg16Extractor>
	{
		std::string ProtoTxtPath = "vgg16-deploy.prototxt";
		std::string CaffeModelPath = "vgg16.caffemodel";
		Device PreferableDevice = Device::cuda;

		void Init()
		{
			const std::string& protoTxtPath = "vgg16-deploy.prototxt";
			const std::string& caffeModelPath = "vgg16.caffemodel";

			const auto protoTxt = LoadFile(protoTxtPath);
			const auto caffeModel = LoadFile(caffeModelPath);

			vgg16_ = cv::dnn::readNetFromCaffe(protoTxt, caffeModel);

			if (PreferableDevice == Device::cpu)
			{
				vgg16_.setPreferableBackend(cv::dnn::DNN_BACKEND_VKCOM);
				vgg16_.setPreferableTarget(cv::dnn::DNN_TARGET_VULKAN);
			}

			if (PreferableDevice == Device::cuda)
			{
				vgg16_.setPreferableBackend(cv::dnn::DNN_BACKEND_CUDA);
				vgg16_.setPreferableTarget(cv::dnn::DNN_TARGET_CUDA);
			}

			if (PreferableDevice == Device::opencl)
			{
				vgg16_.setPreferableBackend(cv::dnn::DNN_BACKEND_DEFAULT);
				vgg16_.setPreferableTarget(cv::dnn::DNN_TARGET_OPENCL);
			}
		}

		decltype(auto) operator()(const RawData& data)
		{
			vgg16_.setInput(cv::dnn::blobFromImage(data.Image.Raw(), 1., cv::Size(224, 224), cv::Scalar(123.68, 116.779, 103.939), false));
			auto feat = vgg16_.forward();
			feat = feat / norm(feat);

			Eigen::Matrix<float, 512, 1> ret{};
			for (auto i = 0; i < 512; ++i)
			{
				ret(i, 0) = feat.at<float>(0, i, 0);
			}

			LogVerb("vgg16: {}, {}, {}, ...", ret(0, 0), ret(1, 0), ret(2, 0));
			return ret;
		}

	private:
		cv::dnn::Net vgg16_;

		static std::vector<uint8_t> LoadFile(const std::string& file)
		{
			try
			{
				return CuFile::ReadAllBytes(file);
			}
			catch (...)
			{
				std::throw_with_nested(Id_MakeExcept("init model file '{}' failed.", file));
			}
		}
	};

	struct OcrExtractor : IExtractor<OcrExtractor>
	{
		std::string Languages = "chi_sim+eng+chi_tra+jpn";

		void Init()
		{
			
		}

		std::u8string operator()(const RawData& data)
		{
			auto api_ = std::make_unique<tesseract::TessBaseAPI>();
			if (api_->Init("tessdata", Languages.c_str())) {
				throw Id_MakeApiExcept("tesseract::TessBaseAPI::Init", "Could not initialize tesseract");
			}

			api_->SetImage(
				data.Image.Data(),
				data.Image.Width(),
				data.Image.Height(),
				decltype(data.Image)::PixelType::ColorSize(),
				data.Image.Linesize());

			auto* text = api_->GetUTF8Text();
			std::u8string ret = reinterpret_cast<char8_t*>(api_->GetUTF8Text());
			LogVerb("ocr: {}", ret);
			delete[] text;

			api_->End();

			return ret;
		}

		~OcrExtractor()
		{

		}

	private:
		std::unique_ptr<tesseract::TessBaseAPI> api_;
	};

	struct BarcodeExtractor : IExtractor<BarcodeExtractor>
	{
		void Init()
		{
			options_ = ZXing::ReaderOptions().setFormats(ZXing::BarcodeFormat::Any);
		}

		std::u8string operator()(const RawData& raw)
		{
			const auto image = ZXing::ImageView(raw.Image.Data(), raw.Image.Width(), raw.Image.Height(), ZXing::ImageFormat::BGR, raw.Image.Linesize(), decltype(raw.Image)::PixelType::ColorSize());
			const auto barcodes = ReadBarcodes(image, options_);
			auto data = nlohmann::json::array();
			for (const auto& b : barcodes) data.push_back({ {ToString(b.format()), b.text()} });
			auto str = data.dump();
			LogVerb("barcode: {}", str);
			return std::u8string(CuStr::FromDirtyUtf8String(str));
		}

	private:
		ZXing::ReaderOptions options_;
	};

	struct OrbExtractor : IExtractor<OrbExtractor>
	{
		void Init()
		{
			orb_ = cv::ORB::create();
		}

		std::vector<uint8_t> operator()(const RawData& raw)
		{
			cv::Mat descriptors;
			std::vector<cv::KeyPoint> keyPoints;
			orb_->detectAndCompute(raw.Image.Raw(), cv::noArray(), keyPoints, descriptors);

			assert(descriptors.type() == CV_8UC1);
			const auto size = descriptors.size().area();
			LogVerb("orb: {}", size);
			std::vector<uint8_t> ret;

			if (size)
			{
				ret.reserve(size);
				ret.resize(size);
				std::copy(descriptors.begin<uint8_t>(), descriptors.end<uint8_t>(), ret.begin());
			}

			return ret;
		}

	private:
		cv::Ptr<cv::ORB> orb_;
	};

	struct SiftExtractor : IExtractor<SiftExtractor>
	{
		void Init()
		{
			orb_ = cv::SIFT::create();
		}

		std::vector<float> operator()(const RawData& raw)
		{
			cv::Mat descriptors;
			std::vector<cv::KeyPoint> keyPoints;
			orb_->detectAndCompute(raw.Image.Raw(), cv::noArray(), keyPoints, descriptors);

			assert(descriptors.type() == CV_32FC1);
			const auto size = descriptors.size().area();
			LogVerb("sift: {}", size);
			std::vector<float> ret;

			if (size)
			{
				ret.reserve(size);
				ret.resize(size);
				std::copy(descriptors.begin<float>(), descriptors.end<float>(), ret.begin());
			}

			return ret;
		}

	private:
		cv::Ptr<cv::SIFT> orb_;
	};

	struct Extractor : IExtractor<Extractor>
	{
		Md5Extractor Md5{};
		Vgg16Extractor Vgg16{};
		OcrExtractor Ocr{};
		BarcodeExtractor Barcode{};
		OrbExtractor Orb{};
		SiftExtractor Sift{};

		struct Row
		{
			std::u8string Path;
			decltype(Md5Extractor{}({})) Md5;
			decltype(Vgg16Extractor{}({})) Vgg16;
			decltype(OcrExtractor{}({})) Ocr;
			decltype(BarcodeExtractor{}({})) Barcode;
			decltype(OrbExtractor{}({})) Orb;
			decltype(SiftExtractor{}({})) Sift;

			operator DataRow()
			{
				return {
					Path,
					Md5,
					Vgg16Type(Vgg16.data(), Vgg16.size()),
					Ocr,
					Barcode,
					Orb,
					Sift
				};
			}
		};

		void Init()
		{
			Md5.Init();
			Vgg16.Init();
			Ocr.Init();
			Barcode.Init();
			Orb.Init();
			Sift.Init();
		}

		Row operator()(const RawData& data)
		{
			return {
				data.Path,
				OperatorWarp(Md5, data),
				OperatorWarp(Vgg16, data),
				OperatorWarp(Ocr, data),
				OperatorWarp(Barcode, data),
				OperatorWarp(Orb, data),
				OperatorWarp(Sift, data)
			};
		}

	private:
		template <typename T>
		struct TimerWarp
		{
			Timer<> timer;

			inline TimerWarp()
			{
				timer = Timer();
			}

			~TimerWarp()
			{
				LogVerb("{} {}ms", typeid(T).name(), timer.Elapse().count());
			}
		};

		template <typename T>
		decltype(auto) OperatorWarp(T& extractor, const RawData& data)
		{
			TimerWarp<T> timer{};
			return extractor(data);
		}
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Device);
