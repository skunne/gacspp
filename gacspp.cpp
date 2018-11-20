#include <iostream>
#include <memory>

#include "CCloudGCP.hpp"
#include "CSimpleSim.hpp"

int main(int argc, char** argv)
{
    IBaseSim::TickType maxTick = 3600 * 24 * 30;
    if(argc>1)
    {
        maxTick = 3600 * 24 * static_cast<IBaseSim::TickType>(std::stoul(argv[1]));
        std::cout<<"MaxTick="<<maxTick<<std::endl;
    }
    std::unique_ptr<CSimpleSim> sim(new CSimpleSim);
    sim->mClouds.emplace_back(new gcp::CCloud("GCP"));
    sim->SetupDefaults();
    sim->Run(maxTick);
	int a;
	std::cin >> a;
}
