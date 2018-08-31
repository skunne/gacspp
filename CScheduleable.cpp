#include "CScheduleable.hpp"

bool SSchedulePrioComparer::operator()(const CScheduleable *left, const CScheduleable *right) const
{
    return left->mNextCallTick > right->mNextCallTick;
}
