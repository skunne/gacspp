#pragma once

#include <vector>


struct SFile
{
    static std::uint64_t IdCounter;
    
    std::uint64_t mId;
    std::uint64_t mExpiresAt;
    std::uint32_t mSize;

    SFile(std::uint64_t expiresAt, std::uint32_t size);
};


class CRucio
{
public:
    std::vector<SFile> mFiles;

    CRucio();
    SFile &CreateFile(std::uint64_t expiresAt, std::uint32_t size);
    std::size_t RunReaper(std::uint64_t now);
};
