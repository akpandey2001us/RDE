using System;

namespace RobbinsDataExportService
{
    [FlagsAttribute]
    enum LoadStatus
    {
        Preparing = 'P',
        Ready = 'R',
        Successful = 'S',
        Failed = 'F',
        BackTrack = 'B',
        Initialize = 'I'
    };

    [FlagsAttribute]
    enum LoadStatusType
    {
        Historic = 'H',
        Delta = 'D'
    }
}
