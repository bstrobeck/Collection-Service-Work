import IGetReportRequestListJob from '../../interfaces/IGetReportRequestListJob';
import IResult from '../../interfaces/IResult';
import MarketplaceIds from '../../enums/MarketplaceIds';
import Reports from '../../services/scheduled/spApi/Reports';
import { getAllAmazonMarketplaceIds } from '../../modules/helpers/request';

export const spGetReportJob = async (params: IGetReportRequestListJob): Promise<IResult> => {
  // Name of current scheduled job for failed job reattempt purposes.
  const job = 'spGetReportJob';
  const service: Reports = new Reports(job, params);
  try {
    const amazonMarketplaceIds: MarketplaceIds[] = await getAllAmazonMarketplaceIds(params.marketplaceId);
    for (const amazonMarketplaceId of amazonMarketplaceIds) {
      await service.setInitParameters(amazonMarketplaceId);
      await service.setMarketplaceId();
      await service.callGetReport(params);
    }

    service.processSuccessfulResponse(`${job} successfully executed for all marketplaces.`);
  } catch (error) {
    service.processError(error);
  }

  return service.responseResult;
};
