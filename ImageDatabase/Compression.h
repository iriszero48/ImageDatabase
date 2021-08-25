namespace Compression
{
    template<typename T>
    class ICompression
    {
    public:
        ICompression(/* args */);
        ~ICompression();

        void Compress();
        void Decompress();
    };
    

}