namespace RobbinsDataExportService
{
    enum LoadStatus
    {
        PreparingLoad = 'P',
        ReadyLoad = 'R',
        SuccessfulLoad = 'S',
        FailedLoad = 'F',
        BackTrack = 'B'
    };

    enum LoadStatusType
    {
        Historic = 'H',
        Delta = 'D'
    }
}
