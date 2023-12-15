import AbstractSpApiService from './AbstractSpApiService';
import Backfill from '../../Backfill';
import DateFormat from '../../../enums/DateFormat';
import ICreateReportRequest from '../../../interfaces/spApi/ICreateReportRequest';
import IGetReportDocumentJob from '../../../interfaces/IGetReportDocumentJob';
import IGetReportListJob from '../../../interfaces/IGetReportListJob';
import IGetReportRequestListJob from '../../../interfaces/IGetReportRequestListJob';
import IGetSplitReportJob from '../../../interfaces/IGetSplitReportJob';
import IRequestReportJob from '../../../interfaces/IRequestReportJob';
import ISpGetReportDocument from '../../../interfaces/spApi/ISpGetReportDocument';
import IStreamProcessData from '../../../interfaces/IStreamProcessData';
import moment from 'moment';
import spHighCapacityReportTypesJson from '../../../config/spHighCapacityReportTypes.json';
import SpReportTables from '../../../enums/SpReportTables';
import TableNames from '../../../enums/TableNames';
import { checkBackfillComplete } from '../../../modules/helpers/backfill';
import { chunkDateRange } from '../../../modules/helpers/request';
import { getClientId, getKnex } from '../../../bootstrap/app';
import { OK } from 'http-status-codes';

export default class Reports extends AbstractSpApiService {
  public async callCreateReport(parameters: IRequestReportJob) {
    if (!this.inputError) {
      this.serviceJob = 'reports/createReport';
      const {
        countryCode,
        endTime,
        endTimeEval,
        isBackfill,
        reportOptions,
        reportType,
        startTime,
        startTimeEval,
      } = parameters;
      let { startTimestamp, endTimestamp } = parameters;

      if (isBackfill) {
        this.isBackfill = isBackfill;

        const backfillParametersObject: any = { reportType };
        if (reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_ASIN'
          || reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_SKU') {
          backfillParametersObject.amazonMarketplaceId = this.requestAmazonMarketplaceId;
        }
        this.backfillParameters = JSON.stringify(backfillParametersObject);
      }
      if (startTimeEval) {
        const startTimeObject = moment.utc().subtract(startTimeEval.subtractMonths, 'months');
        startTimeEval.monthMethod === 'startOf' ? startTimeObject.startOf('month')
          : startTimeObject.endOf('month');
        startTimeEval.dayMethod === 'startOf' ? startTimeObject.startOf('day')
          : startTimeObject.endOf('day');
        startTimestamp = startTimeObject.format(DateFormat.MYSQL_DATE);
      }
      if (endTimeEval) {
        const endTimeObject = moment.utc().subtract(endTimeEval.subtractMonths, 'months');
        endTimeEval.monthMethod === 'startOf' ? endTimeObject.startOf('month')
          : endTimeObject.endOf('month');
        endTimeEval.dayMethod === 'startOf' ? endTimeObject.startOf('day')
          : endTimeObject.endOf('day');
        endTimestamp = endTimeObject.format(DateFormat.MYSQL_DATE);
      }

      this.setLogParametersString(
        startTime,
        endTime,
        startTimestamp,
        endTimestamp,
        reportType,
        countryCode,
        reportOptions,
      );
      this.startTime = await this.setStartTime(startTime, startTimestamp);
      this.endTime = await this.setEndTime(endTime, endTimestamp);

      if (!this.isFirstScheduledRun && (reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_ASIN'
        || reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_SKU')) {
        // GET_SALES_AND_TRAFFIC_REPORT requires a single day per request.
        this.startTime = moment(this.startTime).format(DateFormat.MYSQL_START_OF_DAY);
        this.endTime = moment(this.startTime).format(DateFormat.MYSQL_END_OF_DAY);
      }
      let requestStartTime = this.startTime;
      let requestEndTime = this.endTime;

      /*
       * During the first run of a newly added scheduled job the interval between
       * the request start and end time could be very long, and should be chunked
       * according to the backfill configuration interval.
       */
      if (this.isFirstScheduledRun) {
        const knex = getKnex();
        const isHighVolumeMerchant = Number(await knex(TableNames.MARKETPLACES)
          .where('id', this.marketplaceId)
          .first()
          .pluck('isHighVolumeMerchant'));
        const timeNow = moment.utc();
        const runTime = timeNow.clone().add(15, 'minutes');
        const backfillJobs = new Backfill().getBackfillConfiguration(timeNow);
        for (const backfillJob of backfillJobs) {
          if (backfillJob.spJob === this.job) {
            let configurationReportType = (backfillJob.parameters && backfillJob.parameters.reportType)
              ? backfillJob.parameters.reportType : null;
            // SP API doesn't use '_' to bookend the report type, so we drop those.
            if (configurationReportType && /^_.+_$/.test(configurationReportType)) {
              configurationReportType = configurationReportType.slice(1, -1);
            }
            if (configurationReportType === reportType) {
              const interval = isHighVolumeMerchant
                ? backfillJob.highVolumeChunkInterval : backfillJob.chunkInterval;
              const intervalMinutes = moment.duration(interval).asMinutes();
              const singleDayChunks = (reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_ASIN'
                || reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_SKU') ? true : false;
              const chunks = chunkDateRange(this.startTime, this.endTime, intervalMinutes, false, singleDayChunks);
              // Don't chunk if request size is <= 2 intervals.
              if (chunks.length > 2) {
                let delay = 0;
                let chunkCounter = 0;
                for (const chunk of chunks) {
                  if (chunkCounter === 0) {
                    // First chunk will be requested now, the rest will be queued.
                    requestStartTime = chunk.start;
                    requestEndTime = chunk.end;
                  } else {
                    await knex(TableNames.REATTEMPT_JOBS).insert({
                      marketplaceId: this.marketplaceId,
                      job: this.job,
                      parameters: JSON.stringify({
                        ...backfillJob.parameters,
                        startTimestamp: chunk.start,
                        endTimestamp: chunk.end,
                      }),
                      reattemptAt: runTime.clone()
                        .add(delay += backfillJob.delayMinutes, 'minutes')
                        .format(DateFormat.MYSQL_DATE),
                    });
                  }
                  chunkCounter += 1;
                }
              }
            }
          }
        }
      }

      let apiRequestReportType = reportType;
      // Special case for Sales and Traffic report, change to actual SP-API report type enum.
      if (reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_ASIN'
        || reportType === 'GET_SALES_AND_TRAFFIC_REPORT_BY_SKU') {
        apiRequestReportType = 'GET_SALES_AND_TRAFFIC_REPORT';
      }

      const body: ICreateReportRequest = {
        reportOptions,
        reportType: apiRequestReportType,
        marketplaceIds: [this.requestAmazonMarketplaceId ?? this.amazonMarketplaceId],
        dataStartTime: requestStartTime,
        dataEndTime: requestEndTime,
      };

      this.data = await this.makeSpApiServiceRequest(body);
      await this.logScheduledRequest();

      /*
       * Attempt save only on successful responses.
       * Log any responses that are not successful or throttled.
       */
      this.statusCode = this.data.statusCode;
      if (this.statusCode === OK) {
        if (this.data.results) {
          await this.saveReportRequestDetails(reportType);
        }
      } else {
        await this.handleBadResponse();
      }
      await this.updateReattemptJobSuccess();
    }
  }

  public async callGetReport(params: IGetReportRequestListJob) {
    if (!this.inputError) {
      this.serviceJob = 'reports/getReport';
      const startTime = params.startTime || 3;
      this.logParametersString = JSON.stringify({ startTime });
      const earliestCreatedAt = moment.utc().subtract(startTime, 'days').format(DateFormat.MYSQL_DATE);
      /*
       * Get all the valid reportIds where a reportDocumentId has not been saved yet
       * but only go back specified number of days (default 3), anything beyond
       * that point must be an invalid reportId, this avoids infinite re-requests.
       */
      const reportIds = await this.knex(TableNames.SP_REPORT_REQUESTS)
        .where('marketplaceId', this.marketplaceId)
        .whereIn('spProcessingStatus', ['IN_PROGRESS', 'IN_QUEUE'])
        .where('createdAt', '>=', earliestCreatedAt)
        .whereNull('reportDocumentId')
        .orderBy('createdAt')
        .pluck('spReportId');

      for (const reportId of reportIds) {
        const body = { reportId };
        this.data = await this.makeSpApiServiceRequest(body);

        this.statusCode = this.data.statusCode;
        if (this.statusCode === OK) {
          if (this.data.results) {
            await this.saveGetReportDetails();
          }
        } else {
          await this.handleBadResponse();
        }
      }
    }
  }

  public async callGetReports(parameters: IGetReportListJob) {
    if (!this.inputError) {
      this.serviceJob = 'reports/getReports';
      const {
        startTime,
        endTime,
        startTimestamp,
        endTimestamp,
        reportType,
        isBackfill,
      } = parameters;

      if (isBackfill) {
        this.isBackfill = isBackfill;
        this.backfillParameters = JSON.stringify({ reportType });
      }

      this.setLogParametersString(startTime, endTime, startTimestamp, endTimestamp, reportType);
      this.startTime = await this.setStartTime(startTime, startTimestamp);
      this.endTime = await this.setEndTime(endTime, endTimestamp);

      const body = {
        createdSince: this.startTime,
        createdUntil: this.endTime,
        reportTypes: reportType,
        pageSize: 100,
      };

      this.data = await this.makeSpApiServiceRequest(body);
      await this.logScheduledRequest();

      /*
       * Attempt save only on successful responses.
       */
      this.statusCode = this.data.statusCode;
      if (this.statusCode === OK) {
        if (this.data.results && this.data.results.reports) {
          await this.saveGetReportsDetails();
        }
      } else {
        await this.handleBadResponse();
      }
      await this.updateReattemptJobSuccess();
    }
  }

  public async callGetReportDocument(parameters: IGetReportDocumentJob, context: any = {}) {
    if (!this.inputError) {
      this.serviceJob = 'reports/getReportDocument';
      const highCapacityRun = parameters.highCapacityRun ? parameters.highCapacityRun : false;
      const startTime = parameters.startTime ? parameters.startTime : highCapacityRun ? 7 : 3;
      const earliestCreatedAt = moment.utc().subtract(startTime, 'days').format(DateFormat.MYSQL_DATE);
      const queryLimit = highCapacityRun ? 1 : 60;
      /*
       * Get valid report document ids where a report has not been retrieved yet,
       * but only go back specified number of days (default 3), anything beyond
       * that point must be an invalid report document id, this avoids infinite re-requests.
       * Also ignore currently running or max reattempt timed-out reports.
       */
      const reportIds = await this.knex(TableNames.SP_REPORT_REQUESTS)
        .select(['id', 'spReportType', 'isBackfill', 'timedOut', 'reportDocumentId'])
        .where('marketplaceId', this.marketplaceId)
        .whereNotNull('reportDocumentId')
        .where('spProcessingStatus', 'DONE')
        .modify((queryBuilder) => {
          if (highCapacityRun) {
            queryBuilder.whereIn('spReportType', spHighCapacityReportTypesJson);
          } else {
            queryBuilder.whereNotIn('spReportType', spHighCapacityReportTypesJson);
          }
        })
        .where('createdAt', '>', earliestCreatedAt)
        .whereNull('retrievedAt')
        .where('isRunning', 0)
        .where('timedOut', '<=', 3)
        .orderBy([
          { column: 'isBackfill', order: 'asc' },
          { column: 'createdAt', order: 'asc' },
        ])
        .limit(queryLimit);
      if (reportIds.length) {
        // Empty '/tmp' directory to ensure enough disk space is available.
        await this.emptyTmpDirectory();
        const isHighVolumeMerchant = Number(await this.knex(TableNames.MARKETPLACES)
          .where('id', this.marketplaceId)
          .first()
          .pluck('isHighVolumeMerchant'));
        let counter = 0;
        for (const row of reportIds) {
          if (!SpReportTables[row.spReportType]) {
            Log.error(`Data table not found for SP API report type: ${row.spReportType}. Client id: ${getClientId()}`);
          } else {
            counter += 1;
            let isRunning: number = 0;
            let retrievedAt: string | null = null;
            /*
             * To avoid double-pulling a report in concurrent/overlapping executions,
             * for all but the first report id double check the 'isRunning' and 'retrieved' flags.
             */
            if (counter > 1) {
              const recheckReport = await this.knex(TableNames.SP_REPORT_REQUESTS)
                .select(['isRunning', 'retrievedAt'])
                .where('id', row.id)
                .first();
              isRunning = recheckReport.isRunning;
              retrievedAt = recheckReport.retrievedAt;
            }
            if (Number(isRunning) === 0 && !retrievedAt) {
            /*
             * To avoid partial report insert due to Lambda timeout,
             * do not attempt another report if less than required time left in execution.
             */
              let requiredExecutionMilliseconds = 60000;
              if (isHighVolumeMerchant && row.spReportType === 'GET_LEDGER_DETAIL_VIEW_DATA') {
                requiredExecutionMilliseconds = row.isBackfill ? 210000 : 150000;
              } else if (isHighVolumeMerchant && row.spReportType === 'GET_FLAT_FILES_SALES_TAX_DATA') {
                /*
                 * Since this report does direct inserts due to timeouts with numerous valid duplicate rows
                 * we must avoid timeouts and partial report inserts for data integrity.
                 */
                requiredExecutionMilliseconds = 200000;
              } else if (isHighVolumeMerchant && row.spReportType === 'GET_LEDGER_SUMMARY_VIEW_DATA') {
                requiredExecutionMilliseconds = 450000;
              }
              if (context && context.hasOwnProperty('getRemainingTimeInMillis')
              && context.getRemainingTimeInMillis() < requiredExecutionMilliseconds) break;
              await this.knex(TableNames.SP_REPORT_REQUESTS)
                .update({
                  isRunning: 1,
                  updatedAt: moment.utc().format(DateFormat.MYSQL_DATE),
                })
                .where('id', row.id);
              const reportDocumentId = row.reportDocumentId;
              const body: ISpGetReportDocument = {
                reportDocumentId,
                marketplaceId: this.amazonMarketplaceId,
              };
              this.data = await this.makeSpApiServiceRequest(body);
              // Attempt to save only successful responses.
              this.statusCode = this.data.statusCode;
              const updateReportRequest: any = {};
              if (this.statusCode === OK) {
                if (this.data.results) {
                  await this.saveReport(row.spReportType, reportDocumentId, context);
                  if (row.isBackfill) await checkBackfillComplete();
                }
              } else {
                // Treat errors as timed out requests to limit re-request attempts.
                updateReportRequest.timedOut = row.timedOut + 1;
              }
              updateReportRequest.isRunning = 0;
              updateReportRequest.updatedAt = moment.utc().format(DateFormat.MYSQL_DATE);
              await this.knex(TableNames.SP_REPORT_REQUESTS)
                .update(updateReportRequest)
                .where('id', row.id);
            }
          }
        }
      }
    }
  }

  /**
   * Download and save report in database.
   */
  private async saveReport(spReportType: string, reportDocumentId: string, context: any = {}) {
    try {
      const table = TableNames[SpReportTables[spReportType]];
      if (this.data.results.Bucket && this.data.results.Key) {
        if (await this.checkSaveSpReportToLocalDisk(spReportType)) {
          const filename = `/tmp/${reportDocumentId}.ndjson`;
          const params: IStreamProcessData = {
            table,
            context,
            filename,
            reportTypeEnum: spReportType,
            isSpApiReport: true,
            resumeRowCounter: 0,
          };
          await this.streamDownloadData(filename);
          await this.streamProcessData(params);
          await this.cleanUpTempFiles(filename);
        } else {
          const params: IStreamProcessData = {
            table,
            context,
            filename: '',
            reportTypeEnum: spReportType,
            isSpApiReport: true,
            resumeRowCounter: 0,
          };
          await this.streamProcessData(params);
        }
      } else {
        const message = `Unexpected SP API response for getReportDocument job: ${JSON.stringify(this.data.results)}. `
          + `Client id: ${getClientId()}`;
        Log.error(message);
      }

      const currentTimestamp = moment.utc().format(DateFormat.MYSQL_DATE);
      await this.knex(TableNames.SP_REPORT_REQUESTS)
        .where('marketplaceId', this.marketplaceId)
        .where('reportDocumentId', reportDocumentId)
        .update({
          retrievedAt: currentTimestamp,
          updatedAt: currentTimestamp,
        });
    } catch (error) {
      Log.error(`${error}. Client id: ${getClientId()}`);
    }
  }

  /**
   * Check if a report should be saved to local disk or streamed directly.
   */
  private async checkSaveSpReportToLocalDisk(spReportType: string): Promise<boolean> {
    let saveSpReportToLocalDisk: boolean = true;
    if (spReportType === 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE') {
      const isHighVolumeMerchant = Number(await this.knex(TableNames.MARKETPLACES)
        .where('id', this.marketplaceId)
        .first()
        .pluck('isHighVolumeMerchant'));
      // These reports can exceed 512MB and can't be saved to local /tmp directory, stream directly.
      if (isHighVolumeMerchant) saveSpReportToLocalDisk = false;
    }

    return saveSpReportToLocalDisk;
  }

  /**
   * Save report request details in database.
   */
  private async saveReportRequestDetails(reportType: string) {
    try {
      if (this.data.results.reportId) {
        await this.knex(TableNames.SP_REPORT_REQUESTS).insert({
          marketplaceId: this.marketplaceId,
          scheduledRequestLogId: this.scheduledRequestLogId,
          spReportId: this.data.results.reportId,
          spProcessingStatus: 'IN_PROGRESS',
          spReportType: reportType,
          isBackfill: this.isBackfill,
        });
      }
    } catch (error) {
      Log.error(`${error}. Client id: ${getClientId()}`);
    }
  }

  /**
   * Save Get Report details in database.
   */
  private async saveGetReportDetails() {
    try {
      const results: any = this.data.results;
      const {
        processingStatus,
        reportDocumentId,
        reportId,
      } = results;

      await this.knex(TableNames.SP_REPORT_REQUESTS)
        .where({
          marketplaceId: this.marketplaceId,
          spReportId: reportId,
        })
        .update({
          reportDocumentId: reportDocumentId || null,
          spProcessingStatus: processingStatus,
          updatedAt: moment.utc().format(DateFormat.MYSQL_DATE),
        });

      if (processingStatus === 'CANCELLED') {
        await checkBackfillComplete();
      }
    } catch (error) {
      Log.error(`${error}. Client id: ${getClientId()}`);
    }
  }

  /**
   * Save getReports details in database.
   */
  private async saveGetReportsDetails() {
    try {
      const { reports } = this.data.results;
      if (reports.length) {
        for (const report of reports) {
          await this.saveGetReportsDetailsReport(report);
        }
      } else {
        await this.saveGetReportsDetailsReport(reports);
      }
    } catch (error) {
      Log.error(`${error}. Client id: ${getClientId()}`);
    }
  }

  /**
   * Save getReport details in database.
   */
  private async saveGetReportsDetailsReport(report: any) {
    if (report.reportDocumentId) {
      const clause: any = {
        spReportId: report.reportId,
        spReportType: report.reportType,
        reportDocumentId: report.reportDocumentId,
        spProcessingStatus: 'DONE',
        marketplaceId: this.marketplaceId,
      };
      const request = await this.knex(TableNames.SP_REPORT_REQUESTS)
        .where(clause);

      if (!request.length) {
        await this.knex(TableNames.SP_REPORT_REQUESTS)
          .insert({
            ...clause,
            scheduledRequestLogId: this.scheduledRequestLogId,
            isBackfill: this.isBackfill,
          });
      }
    }
  }

  public async callGetSplitSpReport(parameters: IGetSplitReportJob, context: any = {}) {
    if (!this.inputError) {
      if (!SpReportTables[parameters.reportType]) {
        Log.error(`Data table not found for report type: ${parameters.reportType}. Client id: ${getClientId()}`);
      } else {
        try {
          // Empty '/tmp' directory to ensure enough disk space is available.
          await this.emptyTmpDirectory();
          this.logParametersString = JSON.stringify({
            resumeRowCounter: parameters.resumeRowCounter,
            key: parameters.key,
            reportType: parameters.reportType,
            bucket: parameters.bucket,
          });
          const table = TableNames[SpReportTables[parameters.reportType]];
          this.data = {
            results: {
              Bucket: parameters.bucket,
              Key: parameters.key,
            },
          };

          if (await this.checkSaveSpReportToLocalDisk(parameters.reportType)) {
            const filename = `/tmp/${parameters.key.substring(parameters.key.lastIndexOf('/') + 1)}`;
            const params: IStreamProcessData = {
              table,
              context,
              filename,
              reportTypeEnum: parameters.reportType,
              resumeRowCounter: parameters.resumeRowCounter,
              isSpApiReport: true,
            };
            await this.streamDownloadData(filename);
            await this.streamProcessData(params);
            await this.cleanUpTempFiles(filename);
          } else {
            const params: IStreamProcessData = {
              table,
              context,
              reportTypeEnum: parameters.reportType,
              filename: '',
              resumeRowCounter: parameters.resumeRowCounter,
              isSpApiReport: true,
            };
            await this.streamProcessData(params);
          }

          await this.updateReattemptJobSuccess();
        } catch (error) {
          Log.error(`${error}. Client id: ${getClientId()}`);
          await this.handleBadResponse();
        }
      }
    }
  }
}
