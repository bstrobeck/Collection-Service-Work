import IRequestReportJob from '../../interfaces/IRequestReportJob';
import MarketplaceIds from '../../enums/MarketplaceIds';
import marketplaceTypesJson from '../../config/marketplaceTypes.json';
import Reports from '../../services/scheduled/spApi/Reports';
import {
  getAllAmazonMarketplaceIds,
  isParticipatingAmazonMarketplace,
} from '../../modules/helpers/request';

export const spCreateReportJob = async (parameters: IRequestReportJob) => {
  const job = 'spCreateReportJob';
  const service = new Reports(job, parameters);
  try {
    let amazonMarketplaceIds: MarketplaceIds[] = await getAllAmazonMarketplaceIds(parameters.marketplaceId);
    if (parameters.countryCode) {
      const allowedAmazonMarketplaceIds: MarketplaceIds[] = [];
      for (const code of parameters.countryCode) {
        for (const amazonId in marketplaceTypesJson) {
          const marketplace = marketplaceTypesJson[amazonId];
          if (marketplace.countryCode === code) {
            allowedAmazonMarketplaceIds.push(amazonId as MarketplaceIds);
          }
        }
      }
      amazonMarketplaceIds = amazonMarketplaceIds.filter(
        (amazonMarketplaceId) => allowedAmazonMarketplaceIds.includes(amazonMarketplaceId),
      );
    }

    for (const amazonMarketplaceId of amazonMarketplaceIds) {
      if (parameters.amazonMarketplaceId) {
        const isParticipating = await isParticipatingAmazonMarketplace(
          amazonMarketplaceId,
          parameters.amazonMarketplaceId,
        );
        if (isParticipating) {
          await service.setInitParameters(
            amazonMarketplaceId,
            parameters.amazonMarketplaceId as MarketplaceIds,
          );
          await service.setMarketplaceId();
          await service.callCreateReport(parameters);
        }
      } else {
        await service.setInitParameters(amazonMarketplaceId);
        await service.setMarketplaceId();
        await service.callCreateReport(parameters);
      }
    }

    service.processSuccessfulResponse(`${job} successfully executed for all marketplaces.`);
  } catch (error) {
    service.processError(error);
  }

  return service.responseResult;
};
