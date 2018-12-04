
#pragma once


#define PI (3.14159265359)

#define ONE_MiB (1048576.0) //2^20
#define ONE_GiB (1073741824.0) // 2^30
#define BYTES_TO_GiB(x) ((x) / ONE_GiB)
#define GiB_TO_BYTES(x) ((x) * ONE_GiB)

#define SECONDS_PER_DAY (86400.0) // 60 * 60 * 24
#define SECONDS_PER_MONTH (SECONDS_PER_DAY * 30.0)
#define SECONDS_TO_MONTHS(x) ((x)/SECONDS_PER_MONTH)
#define DAYS_TO_SECONDS(x) ((x) * SECONDS_PER_DAY)

typedef std::uint64_t IdType;
inline IdType GetNewId()
{
    static IdType id = 0;
    return ++id;
}
