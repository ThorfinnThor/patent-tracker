export type Company = {
  companyId: string;
  displayName: string;
  patentCount: number;
  totalCitations: number;
  citationsPerPatent: number;
  cpcBreadth: number;
};

export type PatentRow = {
  patent_id: string;
  patent_date: string;
  patent_title: string;
  patent_num_times_cited_by_us_patents: string;
  cpc_subclass_ids: string;
};
