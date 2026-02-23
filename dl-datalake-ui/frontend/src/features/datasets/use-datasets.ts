import { useQuery } from '@tanstack/react-query';
import client from '../../api/api-client';

export interface Dataset {
    id: string;
    exchange: string;
    symbol: string;
    market: string;
    data_type: string;
    file_path: string;
    file_size_bytes: number;
    last_modified: string;
    time_from?: string;
    time_to?: string;
    timeframe?: string;
}

export interface DatasetList {
    datasets: Dataset[];
    total: number;
}

export interface DataPreview {
    columns: string[];
    rows: any[];
    total_rows: number;
    metadata?: Record<string, any>;
}

export const useDatasets = (filters: {
    exchange?: string;
    symbol?: string;
    data_type?: string;
    limit?: number;
    offset?: number;
} = {}, options: any = {}) => {
    return useQuery<DatasetList>({
        queryKey: ['datasets', filters],
        queryFn: async () => {
            const { data } = await client.get('/datasets', { params: filters });
            return data;
        },
        ...options
    });
};

export const useDatasetPreview = (datasetId: string | null, limit: number = 100, offset: number = 0) => {
    return useQuery<DataPreview>({
        queryKey: ['datasets', datasetId, 'preview', offset],
        queryFn: async () => {
            if (!datasetId) { return { columns: [], rows: [], total_rows: 0 }; }
            const { data } = await client.get(`/datasets/${datasetId}/preview`, { params: { limit, offset } });
            return data;
        },
        enabled: !!datasetId,
    });
};
