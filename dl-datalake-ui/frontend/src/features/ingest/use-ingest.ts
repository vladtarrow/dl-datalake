import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

const API_BASE = 'http://localhost:8000/api/v1/ingest';

export interface Exchange {
    id: string;
    name: string;
}

export interface Market {
    id: string;
    name: string;
}

export interface DownloadRequest {
    exchange: string;
    symbol: string;
    market: string;
    timeframe: string;
    days?: number;
    futures: boolean;
    funding: boolean;
    start_date?: string;
    full_history?: boolean;
    data_type?: 'raw' | 'funding' | 'both';
}

export interface BulkDownloadRequest {
    exchange: string;
    symbols: string[];
    market: string;
    timeframe: string;
    futures: boolean;
    funding: boolean;
    start_date?: string;
    full_history?: boolean;
    data_type?: 'raw' | 'funding' | 'both';
}

export interface TaskState {
    status: 'pending' | 'running' | 'completed' | 'failed';
    message: string;
    exchange: string;
    market: string;
    symbol: string;
    start_time: string;
    data_type?: string;
}

export type DownloadStatus = Record<string, TaskState>;

export const useExchanges = () => {
    return useQuery({
        queryKey: ['exchanges'],
        queryFn: async (): Promise<Exchange[]> => {
            const res = await fetch(`${API_BASE}/exchanges`);
            const data = await res.json();
            if (data.exchanges) {
                localStorage.setItem('cached-exchanges', JSON.stringify(data.exchanges));
            }
            return data.exchanges;
        },
        initialData: () => {
            const saved = localStorage.getItem('cached-exchanges');
            return saved ? JSON.parse(saved) : undefined;
        }
    });
};

export const useMarkets = (exchangeId: string | null) => {
    return useQuery({
        queryKey: ['markets', exchangeId],
        queryFn: async (): Promise<Market[]> => {
            if (!exchangeId) return [];
            const res = await fetch(`${API_BASE}/exchanges/${exchangeId}/markets`);
            const data = await res.json();
            return data.markets;
        },
        enabled: !!exchangeId,
    });
};

export const useSymbols = (exchangeId: string | null, market: string | null) => {
    const cacheKey = `cached-symbols-${exchangeId}-${market}`;
    return useQuery({
        queryKey: ['symbols', exchangeId, market],
        queryFn: async (): Promise<string[]> => {
            if (!exchangeId || !market) return [];
            const res = await fetch(`${API_BASE}/exchanges/${exchangeId}/symbols?market=${market}`);
            const data = await res.json();
            if (data.symbols) {
                localStorage.setItem(cacheKey, JSON.stringify(data.symbols));
            }
            return data.symbols;
        },
        enabled: !!exchangeId && !!market,
        initialData: () => {
            if (!exchangeId || !market) return undefined;
            const saved = localStorage.getItem(cacheKey);
            return saved ? JSON.parse(saved) : undefined;
        }
    });
};

export const useStartDownload = () => {
    return useMutation({
        mutationFn: async (req: DownloadRequest) => {
            const res = await fetch(`${API_BASE}/download`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(req),
            });
            return res.json();
        },
    });
};

export const useBulkDownload = () => {
    return useMutation({
        mutationFn: async (req: BulkDownloadRequest) => {
            const res = await fetch(`${API_BASE}/bulk-download`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(req),
            });
            return res.json();
        },
    });
};

export const useDownloadStatus = () => {
    return useQuery({
        queryKey: ['download-status'],
        queryFn: async (): Promise<DownloadStatus> => {
            const res = await fetch(`${API_BASE}/status`);
            return res.json();
        },
        refetchInterval: 1000, // Poll every 1 second for better responsiveness
    });
};

export const useDeleteSymbol = () => {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async ({ exchange, market, symbol, data_type }: {
            exchange: string;
            market: string;
            symbol: string;
            data_type?: string
        }) => {
            const url = new URL(`${API_BASE}/exchanges/${exchange}/markets/${market}/history`);
            url.searchParams.append('symbol', symbol);
            if (data_type) {
                url.searchParams.append('data_type', data_type);
            }

            const res = await fetch(url.toString(), {
                method: 'DELETE',
            });

            if (!res.ok) {
                const error = await res.json();
                throw new Error(error.detail || 'Failed to delete symbol');
            }

            return res.json();
        },
        onSuccess: () => {
            // Invalidate datasets and status
            queryClient.invalidateQueries({ queryKey: ['datasets'] });
            queryClient.invalidateQueries({ queryKey: ['download-status'] });
        }
    });
};
