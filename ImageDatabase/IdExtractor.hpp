#pragma once

#include <Enum/Enum.hpp>
#include <File/File.hpp>
#include <Cryptography/Md5.hpp>
#include <Cryptography/Base64.hpp>
#include <Image/Image.hpp>

#include <opencv2/opencv.hpp>
#include <opencv2/cudafeatures2d.hpp>
#include <Eigen/Eigen>
#include <tesseract/baseapi.h>
#include <ZXing/ReadBarcode.h>
#include <httplib.h>
#include <nlohmann/json.hpp>

#ifdef ID_ENABLE_RAPID_OCR_NCNN
#include <OcrLite.h>
#endif

#include "IdExcept.hpp"
#include "IdDataset.hpp"
#include "IdLogging.hpp"
#include "IdUtils.hpp"

namespace ImageDatabase
{
	// ReSharper disable CppInconsistentNaming
	CuEnum_MakeEnumDef(Device, cpu, cuda, opencl, vulkan);
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
		using ValueType = std::array<uint8_t, 16>;

		static void Init() {}

		ValueType operator()(const RawData& data) const
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
		using ValueType = Eigen::Matrix<float, 512, 1>;

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

			if (PreferableDevice == Device::vulkan)
			{
				vgg16_.setPreferableBackend(cv::dnn::DNN_BACKEND_VKCOM);
				vgg16_.setPreferableTarget(cv::dnn::DNN_TARGET_VULKAN);
			}
		}

		ValueType operator()(const RawData& data)
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

	struct TesseractOcrExtractor : IExtractor<TesseractOcrExtractor>
	{
		using ValueType = std::u8string;

		std::string Languages = "chi_sim+eng+chi_tra+jpn";

		void Init()
		{
			
		}

		ValueType operator()(const RawData& data)
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

		~TesseractOcrExtractor()
		{

		}

	private:
		std::unique_ptr<tesseract::TessBaseAPI> api_;
	};

	struct PaddleXServingOcrExtractor : IExtractor<PaddleXServingOcrExtractor>
	{
		using ValueType = std::u8string;

		std::string Host = "192.168.0.100";
		uint16_t Port = 8080;
		std::string Url = "/ocr";

		void Init()
		{

		}

		ValueType operator()(const RawData& data)
		{
			if (!client_)
			{
				LogVerb("connect to {}:{}...", Host, Port);
				client_ = std::make_unique<httplib::Client>(Host, Port);
			}

			std::vector<uint8_t> img{};
			cv::imencode(".png", data.Image.Raw(), img);

			CuCrypto::Base64 base64(true);
			auto b64 = base64.Encode(std::string_view(reinterpret_cast<const char *>(img.data()), img.size()));

			const nlohmann::json jsonObj
			{
				{"file", b64},
				{"fileType", 1.},
				{"visualize", false}
			};

			const auto response = client_->Post(Url, jsonObj.dump(), "application/json");
			if (response && response->status == 200) {
				const auto& jsonResponse = nlohmann::json::parse(response->body);
				const auto& result = jsonResponse["result"]["ocrResults"][0]["prunedResult"]["rec_texts"];
				const auto rec_texts = result.get<std::vector<std::string>>();
				const auto text = CuStr::Join(rec_texts.begin(), rec_texts.end(), "\n");
				const auto u8Texts = CuStr::FromDirtyUtf8String(text);
				LogVerb("ocr: {}", CuStr::FromDirtyUtf8String(nlohmann::json(text).dump()));

				return static_cast<std::u8string>(u8Texts);
			}

			if (response) {
				LogErr("HTTP status code: {}", response->status);
			} else {
				LogErr("Failed to send HTTP request.");
			}

			client_.reset();
			return {};
		}

	private:
		std::unique_ptr<httplib::Client> client_{};
	};

	struct RapidOcrNcnnOcrExtractor : IExtractor<RapidOcrNcnnOcrExtractor>
	{
		using ValueType = std::u8string;

		std::string DetPath = "PP_OCRv5_server_det_infer.ncnn";
		std::string ClsPath = "PP_LCNet_x1_0_doc_ori_infer.ncnn";
		std::string RecPath = "PP_OCRv5_server_rec_infer.ncnn";
		std::string KeysPath = "PP-OCRv5_server_rec_infer.yml";

		void Init()
		{
#ifdef ID_ENABLE_RAPID_OCR_NCNN
			auto ocr = std::make_unique<OcrLite>();
			ocr->setNumThread(1);
			ocr->initLogger(true, false, false);
			ocr->setGpuIndex(-1);
			if (!ocr->initModels(DetPath, ClsPath, RecPath, KeysPath))
			{
				throw Id_MakeApiExcept("OcrLite::initModels", "init model failed");
			}

			ocr_ = std::move(ocr);
#else
			assert(false);
#endif
		}

		ValueType operator()(const RawData& data)
		{
#ifdef ID_ENABLE_RAPID_OCR_NCNN
			const auto result = ocr_->detect(data.Image.Raw(),
				50, 1024, 0.5f, 0.3f, 1.6f, true, true);
			const auto resutlU8 = CuStr::FromDirtyUtf8String(result.strRes);
			LogVerb("ocr: {}", resutlU8);
			return std::u8string(resutlU8);
#else
			assert(false);
			return {};
#endif
		}

	private:
#ifdef ID_ENABLE_RAPID_OCR_NCNN
		std::unique_ptr<OcrLite> ocr_{};
#endif
	};

	struct BarcodeExtractor : IExtractor<BarcodeExtractor>
	{
		using ValueType = std::u8string;

		void Init()
		{
			options_ = ZXing::ReaderOptions().setFormats(ZXing::BarcodeFormat::Any);
		}

		ValueType operator()(const RawData& raw)
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

	template <typename Impl>
	struct CvOrbBaseDescExtractor : IExtractor<CvOrbBaseDescExtractor<Impl>>
	{
		using ValueType = std::vector<uint8_t>;

		void Init()
		{
			orb_ = Impl::create();
		}

		ValueType operator()(const RawData& raw)
		{
			cv::cuda::GpuMat gpuData{};

			cv::_InputArray toDet{};
			if constexpr (std::is_same_v<Impl, cv::cuda::ORB>) {
				gpuData.upload(raw.Image.Raw());
				toDet = gpuData;
			} else {
				toDet = raw.Image.Raw();
			}

			cv::Mat descriptors;
			std::vector<cv::KeyPoint> keyPoints;
			orb_->detectAndCompute(toDet, cv::noArray(), keyPoints, descriptors);

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
		cv::Ptr<Impl> orb_;
	};

	using CvOrbDescExtractor = CvOrbBaseDescExtractor<cv::ORB>;
	using CvCudaOrbDescExtractor = CvOrbBaseDescExtractor<cv::cuda::ORB>;

	struct SiftExtractor : IExtractor<SiftExtractor>
	{
		using ValueType = std::vector<float>;

		void Init()
		{
			orb_ = cv::SIFT::create();
		}

		ValueType operator()(const RawData& raw)
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
		using HashExtractor = Md5Extractor;
		using FeatureExtractor = Vgg16Extractor;
		using OcrExtractor = PaddleXServingOcrExtractor;
		// using OcrExtractor = RapidOcrNcnnOcrExtractor;
		using BarcodeExtractor = BarcodeExtractor;
		using DescExtractor = CvOrbDescExtractor;

		HashExtractor Hash{};
		FeatureExtractor Feature{};
		OcrExtractor Ocr{};
		BarcodeExtractor Barcode{};
		DescExtractor Desc{};

		struct Row
		{
			std::u8string Path;
			HashExtractor::ValueType Hash;
			FeatureExtractor::ValueType Feature;
			OcrExtractor::ValueType Ocr;
			BarcodeExtractor::ValueType Barcode;
			DescExtractor::ValueType Desc;

			operator DataRow()
			{
				return {
					Path,
					Hash,
					FeatureType(Feature.data(), Feature.size()),
					Ocr,
					Barcode,
					Desc
				};
			}
		};

		void Init()
		{
			Hash.Init();
			Feature.Init();
			Ocr.Init();
			Barcode.Init();
			Desc.Init();
		}

		Row operator()(const RawData& data)
		{
			return {
				data.Path,
				OperatorWarp(Hash, data),
				OperatorWarp(Feature, data),
				OperatorWarp(Ocr, data),
				OperatorWarp(Barcode, data),
				OperatorWarp(Desc, data)
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

		template <typename T, typename R = typename T::ValueType>
		static R OperatorWarp(T& extractor, const RawData& data)
		{
			TimerWarp<T> timer{};
			return extractor(data);
		}
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Device);
