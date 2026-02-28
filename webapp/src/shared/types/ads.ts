export interface MetaAdsRange {
  since: string;
  until: string;
}

export interface MetaAdsSummary {
  accountCount: number;
  campaignCount: number;
  adSetCount: number;
  adCount: number;
  spend: number;
  impressions: number;
  clicks: number;
  reach: number;
  ctr: number | null;
  cpc: number | null;
}

export interface MetaAdsCampaignRow {
  campaignId: string;
  campaignName: string;
  accountName: string;
  adSetCount: number;
  adCount: number;
  spend: number;
  impressions: number;
  clicks: number;
  reach: number;
  ctr: number | null;
  cpc: number | null;
}

export interface MetaAdsAdSetRow {
  adSetId: string;
  adSetName: string;
  campaignName: string;
  accountName: string;
  adCount: number;
  spend: number;
  impressions: number;
  clicks: number;
  reach: number;
  ctr: number | null;
  cpc: number | null;
}

export interface MetaAdsAdRow {
  accountId: string;
  accountName: string;
  campaignId: string;
  campaignName: string;
  adSetId: string;
  adSetName: string;
  adId: string;
  adName: string;
  spend: number;
  impressions: number;
  clicks: number;
  reach: number;
  ctr: number | null;
  cpc: number | null;
  dateStart: string;
  dateStop: string;
}

export interface MetaAdsOverviewPayload {
  ok: boolean;
  configured: boolean;
  range: MetaAdsRange;
  summary: MetaAdsSummary;
  campaigns: MetaAdsCampaignRow[];
  adsets: MetaAdsAdSetRow[];
  ads: MetaAdsAdRow[];
}
