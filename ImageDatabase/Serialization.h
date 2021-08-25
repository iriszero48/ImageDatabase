#include <fstream>

namespace Serialization
{
    class Serialize
    {
    private:
        std::ostream fs;

        template<bool ToStr, typename T>
        void WriteImpl(const T& val)
        {
            if constexpr (ToStr)
            {
                if constexpr (std::is_arithmetic_v<T>)
                {
                    
                    fs.write((char*)&val, sizeof(T));
                }
                
            }
            
        }
    public:
        Serialize(std::ostream& fs) : fs(fs){};
        ~Serialize();

        template<typename... Args>
        void WriteAsString(Args&&... args)
        {
            (WriteImpl<true>(Args),...);
        }

        template<typename T>
        void Write(const T& val)
        {
            (WriteImpl<false>(Args),...);
        }
    };

    class Unserialize
    {
    private:
        template<typename T>
        T ReadImpl()
        {
            
        }
    public:
        Unserialize(/* args */);
        ~Unserialize();

        template<typename T>
        T ReadFromString()
        {
            
        }

        template<typename T>
        T Read()
        {
            
        }
    };   
}