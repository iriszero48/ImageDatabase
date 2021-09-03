#pragma once

namespace Requires
{
    constexpr int Version[] { 1, 0, 0, 0 };

    struct VersionType
    {
        int Major;
        int Minor;
        int Comp;
        int Dev;
    };

    constexpr bool Require(const int libVer[4], const VersionType reqVer)
    {
        return
            libVer[0] == reqVer.Major &&
            libVer[1] == reqVer.Minor &&
            libVer[2] >= reqVer.Comp;
    }
}
