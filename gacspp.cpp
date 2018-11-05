#include <iostream>
#include <memory>

#include "CCloudGCP.hpp"
#include "CSimpleSim.hpp"

int main()
{
    std::unique_ptr<CSimpleSim> sim(new CSimpleSim);
    sim->mClouds.emplace_back(new gcp::CCloud("GCP"));
    sim->SetupDefaults();
    sim->Run(3600 * 24 * 31);
	int a;
	std::cin >> a;
}
