using System;

namespace RobbinsExportBusinessLayer
{
    [FlagsAttribute]
    public enum LoadStatus
    {
        Preparing = 'P',
        Ready = 'R',
        Successful = 'S',
        Failed = 'F',
        BackTrack = 'B',
        Initialize = 'I'
    };

    [FlagsAttribute]
    public enum LoadStatusType
    {
        Historic = 'H',
        Delta = 'D'
    }
}
