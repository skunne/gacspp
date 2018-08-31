#include "CRucio.hpp"


std::uint64_t SFile::IdCounter = 0;

SFile::SFile(std::uint64_t expiresAt, std::uint32_t size)
:   mId(++IdCounter),
    mExpiresAt(expiresAt),
    mSize(size)
{}


CRucio::CRucio()
{
    mFiles.reserve(65536);
}

SFile &CRucio::CreateFile(std::uint64_t expiresAt, std::uint32_t size)
{
    mFiles.emplace_back(expiresAt, size);
    return mFiles.back();
}

std::size_t CRucio::RunReaper(std::uint64_t now)
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now)
    {
        mFiles.pop_back();
        --backIdx;
    }

    while(frontIdx < backIdx)
    {
        if(mFiles[frontIdx].mExpiresAt <= now)
        {
            mFiles[frontIdx] = std::move(mFiles[backIdx]);
            do
            {
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now);
        }
        ++frontIdx;
    }

    if(backIdx == 0)
    {
        if(mFiles.back().mExpiresAt <= now)
            mFiles.pop_back();
    }

    return numFiles - mFiles.size();
}
