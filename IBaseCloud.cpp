#include "IBaseCloud.hpp"
#include "ISite.hpp"

IBaseCloud::IBaseCloud(std::string&& name)
    : mName(std::move(name))
{}

IBaseCloud::~IBaseCloud() = default;
