#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include <execution>
#include <climits>

namespace __Detail
{
	namespace Bit
	{
		enum class Endian
		{
#ifdef _MSC_VER
			Little = 0,
			Big = 1,
			Native = Little
#else
			Little = __ORDER_LITTLE_ENDIAN__,
			Big = __ORDER_BIG_ENDIAN__,
			Native = __BYTE_ORDER__
#endif
		};

		template<typename T, std::size_t... N>
		constexpr T BSwapImpl(T i, std::index_sequence<N...>)
		{
			return (((i >> N * CHAR_BIT & static_cast<std::uint8_t>(-1)) << (sizeof(T) - 1 - N) * CHAR_BIT) | ...);
		}

		template<typename T, typename U = std::make_unsigned_t<T>>
		constexpr U BSwap(T i)
		{
			return BSwapImpl<U>(i, std::make_index_sequence<sizeof(T)>{});
		}
	}

	namespace Enumerable
	{
		template<typename T>
		class Range
		{
			T begVal = 0;
			T endVal;

		public:
			class iterator
			{
				T val;

			public:
				using iterator_category = std::random_access_iterator_tag;
				using value_type = T;
				using difference_type = std::uint64_t;
				using pointer = const T*;
				using reference = const T;

				iterator() : val(0) {}
				iterator(T val) : val(val) {}

				iterator& operator++() { ++val; return *this; }
				iterator operator++(int) { iterator retVal = *this; ++(*this); return retVal; }

				bool operator==(iterator other) const { return val == other.val; }
				bool operator!=(iterator other) const { return !(*this == other); }
				bool operator<(iterator other) const { return val < other.val; }

				reference operator*() const { return val; }
				value_type operator+(iterator other) const { return val + other.val; }
				difference_type operator-(iterator other) const { return val - other.val; }

				reference operator[](const difference_type off) const { return val + off; }
			};

			Range(T count) :endVal(count) {}
			Range(T start, T count) : begVal(start), endVal(start + count) {}

			iterator begin() { return iterator(begVal); }
			iterator end() { return iterator(endVal); }
		};
	}
}

namespace Cryptography
{
	template<typename ValueType>
	class Hash
	{
	public:
		using HashValueType = ValueType;

		HashValueType Data{};

		Hash(const HashValueType& val);
		operator std::string() const;

		bool operator==(const std::string& hashStr) const;

		bool operator!=(const std::string& hashStr) const
		{
			return !(*this == hashStr);
		}

		bool operator==(const Hash<HashValueType>& hash) const;

		bool operator!=(const Hash<HashValueType>& hash) const
		{
			return !(*this == hash);
		}
	};

	template<typename T, typename Type>
	class IHashAlgorithm
	{
	public:
		using HashValueType = Type;
		using HashType = Hash<HashValueType>;

		void Append(std::uint8_t* buf, std::uint64_t len)
		{
			static_cast<T*>(this)->Append(buf, len);
		}
		void Append(std::istream& stream)
		{
			static_cast<T*>(this)->Append(stream);
		}
		HashType Digest()
		{
			return static_cast<T*>(this)->Digest;
		}
	};

	class Md5 : IHashAlgorithm<Md5, std::uint8_t[16]>
	{
	public:
		using HashValueType = HashValueType;

		Md5();
		void Append(std::uint8_t* buf, std::uint64_t len);
		void Append(std::istream& stream);
		HashType Digest();

	private:
		union DigestData
		{
			struct Integer { std::uint32_t A, B, C, D; } DWord;
			std::uint8_t Word[16];
		};

		DigestData data;
		std::uint8_t buffer[64]{ 0 };
		std::uint8_t bufferLen = 0;
		std::uint64_t length = 0;
		bool finished = false;

		void Append64(std::uint8_t* buf, std::uint64_t n);
	};
}
