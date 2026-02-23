import { useState, useEffect, useMemo } from 'react';
import { Download, FileUp, Loader2, Search, CheckCircle2, Globe, BarChart3, Clock, AlertCircle, ChevronDown, ChevronUp, ArrowUpDown, ArrowUp, ArrowDown, DollarSign, Trash2 } from 'lucide-react';
import { useExchanges, useMarkets, useSymbols, useStartDownload, useBulkDownload, useDownloadStatus, useDeleteSymbol, type Exchange } from './use-ingest';
import { useDatasets } from '../datasets/use-datasets';
import { useQueryClient } from '@tanstack/react-query';

export const IngestionForm = () => {
    const queryClient = useQueryClient();
    const [activeTab, setActiveTab] = useState<'download' | 'file'>('download');

    // State for discovery
    const [selectedExchange, setSelectedExchange] = useState<string>('binance');
    const [selectedMarket, setSelectedMarket] = useState<string>('');
    const [exchangeSearch, setExchangeSearch] = useState('');
    const [symbolSearch, setSymbolSearch] = useState('');
    const [startDate, setStartDate] = useState<string>('2025-01-01');
    const [fullHistory, setFullHistory] = useState<boolean>(true);
    const [isCollapsed, setIsCollapsed] = useState<boolean>(false);
    const [sortConfig, setSortConfig] = useState<{ key: 'symbol' | 'status' | 'first' | 'last', direction: 'asc' | 'desc' }>({ key: 'symbol', direction: 'asc' });

    // Column widths for the symbol list
    const [columnWidths, setColumnWidths] = useState<{ [key: string]: number }>({
        first: 192,
        last: 192,
        action: 120
    });

    const { data: exchanges, isLoading: loadingExchanges } = useExchanges();
    const { data: downloadStatus } = useDownloadStatus(); // Moved up for refetchInterval dependency
    const { data: datasets } = useDatasets({
        exchange: selectedExchange,
        limit: 10000 // Fetch all datasets for this exchange to populate status/dates
    }, {
        refetchInterval: Object.values(downloadStatus || {}).some(t => t.status === 'running') ? 3000 : false
    });
    const { data: markets, isLoading: loadingMarkets } = useMarkets(selectedExchange);
    const { data: symbols, isLoading: loadingSymbols } = useSymbols(selectedExchange, selectedMarket);

    const startDownload = useStartDownload();
    const bulkDownload = useBulkDownload();
    const deleteSymbol = useDeleteSymbol();

    const handleDeleteHistory = (symbol: string) => {
        if (!confirm(`Are you sure you want to delete HISTORY for ${symbol}? This cannot be undone.`)) return;

        setMessage(null);
        deleteSymbol.mutate({
            exchange: selectedExchange,
            market: selectedMarket,
            symbol: symbol
        }, {
            onSuccess: () => {
                setMessage({ type: 'success', text: `Deleted history for ${symbol}` });
            },
            onError: (err: any) => {
                setMessage({ type: 'error', text: `Failed to delete ${symbol}: ${err.message}` });
            }
        });
    };

    const [message, setMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null);

    // Persistence: Load from localStorage
    useEffect(() => {
        const savedSettings = localStorage.getItem('ui-settings');
        if (savedSettings) {
            try {
                const settings = JSON.parse(savedSettings);
                if (settings.exchange) setSelectedExchange(settings.exchange);
                if (settings.market) setSelectedMarket(settings.market);
                if (settings.startDate) setStartDate(settings.startDate);
                if (settings.fullHistory !== undefined) setFullHistory(settings.fullHistory);
                if (settings.isCollapsed !== undefined) setIsCollapsed(settings.isCollapsed);
            } catch (e) {
                console.error('Failed to parse ui-settings from localStorage', e);
            }
        }

        const savedWidths = localStorage.getItem('column-widths');
        if (savedWidths) {
            try {
                setColumnWidths(JSON.parse(savedWidths));
            } catch (e) {
                console.error('Failed to parse column-widths from localStorage', e);
            }
        }
    }, []);

    // Persistence: Save to localStorage
    useEffect(() => {
        const settings = {
            exchange: selectedExchange,
            market: selectedMarket,
            startDate: startDate,
            fullHistory: fullHistory,
            isCollapsed: isCollapsed
        };
        localStorage.setItem('ui-settings', JSON.stringify(settings));
    }, [selectedExchange, selectedMarket, startDate, fullHistory, isCollapsed]);

    useEffect(() => {
        localStorage.setItem('column-widths', JSON.stringify(columnWidths));
    }, [columnWidths]);

    const handleResize = (id: string, startX: number, startWidth: number) => {
        const onMouseMove = (e: MouseEvent) => {
            const delta = e.clientX - startX;
            setColumnWidths(prev => ({
                ...prev,
                [id]: Math.max(80, startWidth + delta)
            }));
        };

        const onMouseUp = () => {
            document.removeEventListener('mousemove', onMouseMove);
            document.removeEventListener('mouseup', onMouseUp);
            document.body.style.cursor = 'default';
        };

        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', onMouseUp);
        document.body.style.cursor = 'col-resize';
    };

    const ResizeHandle = ({ columnId, width }: { columnId: string, width: number }) => (
        <div
            className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-blue-500/50 transition-colors z-10"
            onMouseDown={(e) => {
                e.preventDefault();
                e.stopPropagation();
                handleResize(columnId, e.clientX, width);
            }}
        />
    );

    const filteredExchanges = useMemo(() =>
        exchanges?.filter((ex: Exchange) =>
            ex.name.toLowerCase().includes(exchangeSearch.toLowerCase()) ||
            ex.id.toLowerCase().includes(exchangeSearch.toLowerCase())
        ), [exchanges, exchangeSearch]);

    const downloadedMetadata = useMemo(() => {
        const result = new Map<string, { time_from: string, time_to: string }>();
        if (!datasets?.datasets) return result;

        datasets.datasets.forEach((d: any) => {
            if (d.exchange.toLowerCase() === selectedExchange.toLowerCase() &&
                d.market.toLowerCase() === selectedMarket.toLowerCase()) {
                const existing = result.get(d.symbol);
                if (!existing) {
                    result.set(d.symbol, {
                        time_from: d.time_from || null,
                        time_to: d.time_to || null
                    });
                } else {
                    // Use ISO string comparison for dates
                    if (d.time_from && (!existing.time_from || d.time_from < existing.time_from)) {
                        existing.time_from = d.time_from;
                    }
                    if (d.time_to && (!existing.time_to || d.time_to > existing.time_to)) {
                        existing.time_to = d.time_to;
                    }
                }
            }
        });
        return result;
    }, [datasets, selectedExchange, selectedMarket]);

    const filteredSymbols = useMemo(() => {
        if (!symbols) return [];
        let result = symbols.filter((s: string) => s.toLowerCase().includes(symbolSearch.toLowerCase()));

        result.sort((a: string, b: string) => {
            if (sortConfig.key === 'symbol') {
                return sortConfig.direction === 'asc' ? a.localeCompare(b) : b.localeCompare(a);
            }

            if (sortConfig.key === 'status') {
                const getStatusPriority = (s: string) => {
                    const sanitized = s.replace(/[\/:]/g, '_');

                    const taskKeyRaw = `${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${s}:raw`;
                    const taskKeyFunding = `${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${s}:funding`;

                    const taskRaw = downloadStatus?.[taskKeyRaw];
                    const taskFunding = downloadStatus?.[taskKeyFunding];

                    // Legacy check
                    const taskLegacy = downloadStatus?.[`${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${s}`];

                    const running = taskRaw?.status === 'running' || taskFunding?.status === 'running' || taskLegacy?.status === 'running';
                    const pending = taskRaw?.status === 'pending' || taskFunding?.status === 'pending' || taskLegacy?.status === 'pending';
                    const failed = taskRaw?.status === 'failed' || taskFunding?.status === 'failed';

                    const metadata = downloadedMetadata.get(sanitized) || downloadedMetadata.get(s);

                    if (running) return 0;
                    if (pending) return 1;
                    if (!!metadata) return 2;
                    if (failed) return 3;
                    return 4;
                };
                const pA = getStatusPriority(a);
                const pB = getStatusPriority(b);
                if (pA !== pB) return sortConfig.direction === 'asc' ? pA - pB : pB - pA;
                return a.localeCompare(b);
            }

            if (sortConfig.key === 'first' || sortConfig.key === 'last') {
                const sanitize = (s: string) => s.replace(/[\/:]/g, '_');
                const metaA = downloadedMetadata.get(sanitize(a)) || downloadedMetadata.get(a);
                const metaB = downloadedMetadata.get(sanitize(b)) || downloadedMetadata.get(b);
                if (sortConfig.key === 'first') {
                    const dateA = metaA?.time_from ? new Date(metaA.time_from).getTime() : 0;
                    const dateB = metaB?.time_from ? new Date(metaB.time_from).getTime() : 0;
                    return sortConfig.direction === 'asc' ? dateA - dateB : dateB - dateA;
                }
                if (sortConfig.key === 'last') {
                    const dateA = metaA?.time_to ? new Date(metaA.time_to).getTime() : 0;
                    const dateB = metaB?.time_to ? new Date(metaB.time_to).getTime() : 0;
                    return sortConfig.direction === 'asc' ? dateA - dateB : dateB - dateA;
                }
            }

            return 0;
        });

        return result;
    }, [symbols, symbolSearch, sortConfig, downloadStatus, downloadedMetadata, selectedExchange, selectedMarket]);

    useEffect(() => {
        if (markets && markets.length > 0) {
            if (!selectedMarket || !markets.find(m => m.id === selectedMarket)) {
                setSelectedMarket(markets[0].id);
            }
        }
    }, [markets]);

    const handleQuickDownload = (symbol: string, type: 'raw' | 'funding') => {
        setMessage(null);
        const isDerivative = ['future', 'swap', 'linear', 'derivative'].some(d => selectedMarket.toLowerCase().includes(d));

        startDownload.mutate({
            exchange: selectedExchange,
            symbol: symbol,
            market: selectedMarket,
            timeframe: '1m',
            futures: isDerivative,
            funding: type === 'funding',
            start_date: fullHistory ? undefined : (startDate || undefined),
            full_history: fullHistory,
            data_type: type
        }, {
            onSuccess: () => {
                setMessage({ type: 'success', text: `Queued ${symbol} ${type} for download` });
            },
            onError: () => {
                setMessage({ type: 'error', text: `Failed to queue ${type} download for ${symbol}` });
            }
        });
    };

    const handleDownloadAll = (type: 'raw' | 'funding') => {
        if (!filteredSymbols || filteredSymbols.length === 0) return;

        const isDerivative = ['future', 'swap', 'linear', 'derivative'].some(d => selectedMarket.toLowerCase().includes(d));

        bulkDownload.mutate({
            exchange: selectedExchange,
            symbols: filteredSymbols,
            market: selectedMarket,
            timeframe: '1m',
            futures: isDerivative,
            funding: type === 'funding',
            start_date: fullHistory ? undefined : (startDate || undefined),
            full_history: fullHistory,
            data_type: type
        }, {
            onSuccess: () => {
                setMessage({ type: 'success', text: `Queued ${filteredSymbols.length} symbols for ${type} download` });
            },
            onError: () => {
                setMessage({ type: 'error', text: `Failed to queue bulk ${type} download` });
            }
        });
    };

    const formatDate = (dateStr?: string) => {
        if (!dateStr) return '—';
        return new Date(dateStr).toLocaleDateString('en-GB', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    return (
        <div className="flex flex-col h-[calc(100vh-160px)] -mt-2">
            {/* Global Settings Bar */}
            <div className="bg-gray-950 border border-gray-800 rounded-xl p-4 mb-4 flex items-center justify-between">
                <div className="flex bg-gray-900 p-1 rounded-lg">
                    <button
                        onClick={() => setActiveTab('download')}
                        className={`flex items-center gap-2 px-4 py-1.5 rounded-md text-sm transition-all ${activeTab === 'download' ? 'bg-blue-600 text-white shadow-lg' : 'text-gray-400 hover:text-gray-200'}`}
                    >
                        <Download className="w-3.5 h-3.5" />
                        <span>History Download</span>
                    </button>
                    <button
                        onClick={() => setActiveTab('file')}
                        className={`flex items-center gap-2 px-4 py-1.5 rounded-md text-sm transition-all ${activeTab === 'file' ? 'bg-blue-600 text-white shadow-lg' : 'text-gray-400 hover:text-gray-200'}`}
                    >
                        <FileUp className="w-3.5 h-3.5" />
                        <span>Manual Ingest</span>
                    </button>
                </div>

                {activeTab === 'download' && (
                    <div className="flex items-center gap-6">
                        <div className="flex items-center gap-2">
                            <label className="text-xs font-medium text-gray-500 uppercase tracking-wider">Resolution</label>
                            <span className="text-sm font-bold text-gray-400 bg-gray-900 px-3 py-1.5 rounded-lg border border-gray-800">1min</span>
                        </div>

                        <div className="flex items-center gap-2">
                            <label className="text-xs font-medium text-gray-500 uppercase tracking-wider">Sync From</label>
                            <div className="flex items-center gap-3 bg-gray-900 px-3 py-1.5 rounded-lg border border-gray-800">
                                <div className="relative">
                                    <input
                                        type="date"
                                        value={startDate}
                                        onChange={(e) => setStartDate(e.target.value)}
                                        disabled={fullHistory}
                                        className={`bg-transparent text-sm font-bold text-gray-300 outline-none transition-all [color-scheme:dark] ${fullHistory ? 'opacity-30 grayscale cursor-not-allowed' : ''}`}
                                    />
                                </div>
                                <div className="w-px h-4 bg-gray-800 mx-1" />
                                <label className="flex items-center gap-2 cursor-pointer group">
                                    <div className="relative flex items-center">
                                        <input
                                            type="checkbox"
                                            checked={fullHistory}
                                            onChange={(e) => setFullHistory(e.target.checked)}
                                            className="sr-only peer"
                                        />
                                        <div className="w-8 h-4 bg-gray-800 rounded-full peer peer-checked:bg-blue-600 transition-colors" />
                                        <div className="absolute left-0.5 top-0.5 w-3 h-3 bg-gray-400 rounded-full transition-all peer-checked:translate-x-4 peer-checked:bg-white" />
                                    </div>
                                    <span className={`text-[10px] font-bold uppercase tracking-tight transition-colors ${fullHistory ? 'text-blue-400' : 'text-gray-500 group-hover:text-gray-400'}`}>Full History</span>
                                </label>
                            </div>
                        </div>
                    </div>
                )}
            </div>

            <div className="flex flex-1 gap-6 min-h-0">
                {activeTab === 'download' ? (
                    <>
                        {/* Sidebar: Exchanges */}
                        <div className="w-64 bg-gray-950 border border-gray-800 rounded-2xl flex flex-col overflow-hidden">
                            <div className="p-4 border-b border-gray-800 bg-gray-900/50">
                                <div className="relative">
                                    <Search className="w-4 h-4 text-gray-500 absolute left-3 top-3" />
                                    <input
                                        placeholder="Exchange..."
                                        value={exchangeSearch}
                                        onChange={(e) => setExchangeSearch(e.target.value)}
                                        className="w-full bg-gray-900 border border-gray-800 rounded-xl py-2 pl-9 pr-3 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                                    />
                                </div>
                            </div>
                            <div className="flex-1 overflow-y-auto custom-scrollbar p-2 space-y-1">
                                {loadingExchanges ? (
                                    <div className="flex justify-center py-10"><Loader2 className="animate-spin text-gray-700" /></div>
                                ) : (
                                    filteredExchanges?.map((ex: Exchange) => (
                                        <button
                                            key={ex.id}
                                            onClick={() => setSelectedExchange(ex.id)}
                                            className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-left text-sm transition-all group ${selectedExchange === ex.id
                                                ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/20'
                                                : 'text-gray-400 hover:bg-white/5 hover:text-gray-200'
                                                }`}
                                        >
                                            <Globe className={`w-4 h-4 ${selectedExchange === ex.id ? 'text-white' : 'text-gray-600 group-hover:text-blue-400'}`} />
                                            <span className="font-medium truncate">{ex.name}</span>
                                        </button>
                                    ))
                                )}
                            </div>
                        </div>

                        {/* Main Content: Markets + Symbols */}
                        <div className="flex-1 flex flex-col bg-gray-950 border border-gray-800 rounded-2xl overflow-hidden">
                            {/* Horizontal Tabs: Markets */}
                            <div className="p-2 bg-gray-900/50 flex items-center gap-2 border-b border-gray-800">
                                {loadingMarkets ? (
                                    <div className="h-8 w-20 bg-gray-800 rounded-lg animate-pulse" />
                                ) : (
                                    markets?.map(m => (
                                        <button
                                            key={m.id}
                                            onClick={() => setSelectedMarket(m.id)}
                                            className={`px-6 py-2 rounded-xl text-sm font-bold transition-all ${selectedMarket === m.id
                                                ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20 shadow-[0_0_15px_-5px_theme(colors.blue.500)]'
                                                : 'text-gray-500 hover:text-gray-300'
                                                }`}
                                        >
                                            {m.name}
                                        </button>
                                    ))
                                )}
                            </div>

                            {/* Symbol Search + Status */}
                            <div className="p-4 border-b border-gray-800 bg-gray-950/50 flex items-center justify-between gap-4">
                                <div className="relative flex-1">
                                    <Search className="w-4 h-4 text-gray-500 absolute left-4 top-3.5" />
                                    <input
                                        placeholder={`Search symbols...`}
                                        value={symbolSearch}
                                        onChange={(e) => setSymbolSearch(e.target.value)}
                                        className="w-full bg-gray-900 border border-gray-800 rounded-2xl py-3 pl-11 pr-4 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                                    />
                                </div>

                                <div className="flex items-center gap-2">
                                    <button
                                        onClick={() => handleDownloadAll('raw')}
                                        disabled={!filteredSymbols?.length || startDownload.isPending}
                                        className="px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-800 disabled:text-gray-500 text-white rounded-xl font-bold text-xs flex items-center gap-2 transition-all shadow-lg shadow-blue-500/20"
                                    >
                                        <BarChart3 className="w-3.5 h-3.5 fill-current" />
                                        <span>Candles ({filteredSymbols?.length || 0})</span>
                                    </button>
                                    {['future', 'swap', 'linear', 'derivative'].some(d => selectedMarket.toLowerCase().includes(d)) && (
                                        <button
                                            onClick={() => handleDownloadAll('funding')}
                                            disabled={!filteredSymbols?.length || startDownload.isPending}
                                            className="px-4 py-3 bg-purple-600 hover:bg-purple-700 disabled:bg-gray-800 disabled:text-gray-500 text-white rounded-xl font-bold text-xs flex items-center gap-2 transition-all shadow-lg shadow-purple-500/20"
                                        >
                                            <DollarSign className="w-3.5 h-3.5" />
                                            <span>Funding ({filteredSymbols?.length || 0})</span>
                                        </button>
                                    )}
                                </div>

                                {message && (
                                    <div className={`px-4 py-2.5 rounded-xl border text-xs font-bold animate-in fade-in slide-in-from-right-2 ${message.type === 'success' ? 'bg-green-600/10 border-green-500/20 text-green-400' : 'bg-red-600/10 border-red-500/20 text-red-400'
                                        }`}>
                                        {message.text}
                                    </div>
                                )}
                            </div>

                            {/* Symbols Header */}
                            <div className="px-6 py-2 border-b border-gray-800 bg-gray-900/30 flex items-center text-[10px] font-bold text-gray-500 uppercase tracking-widest select-none">
                                <div
                                    className="flex-1 flex items-center gap-1 cursor-pointer hover:text-gray-300 transition-colors"
                                    onClick={() => setSortConfig(prev => ({ key: 'symbol', direction: prev.key === 'symbol' && prev.direction === 'asc' ? 'desc' : 'asc' }))}
                                >
                                    Symbol & Status
                                    {sortConfig.key === 'symbol' ? (sortConfig.direction === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />) : <ArrowUpDown className="w-3 h-3 opacity-30" />}
                                </div>

                                <div className="relative flex items-center h-full flex-shrink-0" style={{ width: columnWidths.first }}>
                                    <div
                                        className="w-full flex items-center justify-center gap-1 cursor-pointer hover:text-gray-300 transition-colors"
                                        onClick={() => setSortConfig(prev => ({ key: 'first', direction: prev.key === 'first' && prev.direction === 'asc' ? 'desc' : 'asc' }))}
                                    >
                                        First Candle
                                        {sortConfig.key === 'first' ? (sortConfig.direction === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />) : <ArrowUpDown className="w-3 h-3 opacity-30" />}
                                    </div>
                                    <ResizeHandle columnId="first" width={columnWidths.first} />
                                </div>

                                <div className="relative flex items-center h-full flex-shrink-0" style={{ width: columnWidths.last }}>
                                    <div
                                        className="w-full flex items-center justify-center gap-1 cursor-pointer hover:text-gray-300 transition-colors"
                                        onClick={() => setSortConfig(prev => ({ key: 'last', direction: prev.key === 'last' && prev.direction === 'asc' ? 'desc' : 'asc' }))}
                                    >
                                        Last Candle
                                        {sortConfig.key === 'last' ? (sortConfig.direction === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />) : <ArrowUpDown className="w-3 h-3 opacity-30" />}
                                    </div>
                                    <ResizeHandle columnId="last" width={columnWidths.last} />
                                </div>

                                <div className="relative flex items-center h-full ml-4 flex-shrink-0" style={{ width: columnWidths.action }}>
                                    <div
                                        className="w-full flex items-center gap-1 cursor-pointer hover:text-gray-300 transition-colors"
                                        onClick={() => setSortConfig(prev => ({ key: 'status', direction: prev.key === 'status' && prev.direction === 'asc' ? 'desc' : 'asc' }))}
                                    >
                                        Action
                                        {sortConfig.key === 'status' ? (sortConfig.direction === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />) : <ArrowUpDown className="w-3 h-3 opacity-30" />}
                                    </div>
                                    <ResizeHandle columnId="action" width={columnWidths.action} />
                                </div>
                            </div>

                            {/* Symbols List */}
                            <div className="flex-1 overflow-y-auto custom-scrollbar p-4">
                                {loadingSymbols ? (
                                    <div className="space-y-2">
                                        {[...Array(9)].map((_, i) => (
                                            <div key={i} className="h-14 bg-gray-900 rounded-xl animate-pulse" />
                                        ))}
                                    </div>
                                ) : (
                                    <div className="space-y-2">
                                        {filteredSymbols?.map((symbol: string) => {
                                            // Sanitize symbol to match Manifest DB format (underscores)
                                            const sanitizedSymbol = symbol.replace(/[\/:]/g, '_');
                                            const metadata = downloadedMetadata.get(sanitizedSymbol) || downloadedMetadata.get(symbol);

                                            const taskKeyRaw = `${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${symbol}:raw`;
                                            const taskKeyFunding = `${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${symbol}:funding`;

                                            // Handle legacy active tasks or 'both' if any
                                            const taskKeyLegacy = `${selectedExchange.toLowerCase()}:${selectedMarket.toLowerCase()}:${symbol}`;

                                            const taskRaw = downloadStatus?.[taskKeyRaw];
                                            const taskFunding = downloadStatus?.[taskKeyFunding];
                                            const taskLegacy = downloadStatus?.[taskKeyLegacy];

                                            const isDownloadingRaw = taskRaw?.status === 'running' || taskRaw?.status === 'pending' || (taskLegacy?.status === 'running' && !taskLegacy.data_type && !taskLegacy.message?.includes('funding'));
                                            const isDownloadingFunding = taskFunding?.status === 'running' || taskFunding?.status === 'pending';

                                            const isDownloadingAny = isDownloadingRaw || isDownloadingFunding;

                                            const isLoaded = !!metadata && !!metadata.time_from && !!metadata.time_to;

                                            // Status Badge Logic
                                            let statusBadge = (
                                                <div className="text-[10px] text-gray-500 font-bold bg-gray-800/50 px-1.5 py-0.5 rounded uppercase tracking-wider">
                                                    Empty
                                                </div>
                                            );

                                            if (isDownloadingAny) {
                                                const activeTask = taskRaw || taskFunding || taskLegacy;
                                                statusBadge = (
                                                    <div className={`flex items-center gap-1.5 text-[10px] font-bold bg-blue-500/10 px-1.5 py-0.5 rounded uppercase tracking-wider ${activeTask?.status === 'running' ? 'text-blue-400' : 'text-yellow-400'}`}>
                                                        {activeTask?.status === 'running' ? <Loader2 className="w-2.5 h-2.5 animate-spin" /> : <Clock className="w-2.5 h-2.5" />}
                                                        {activeTask?.status === 'running' ? 'Downloading...' : 'In Queue'}
                                                    </div>
                                                );
                                            } else if (taskRaw?.status === 'failed' || taskFunding?.status === 'failed') {
                                                statusBadge = (
                                                    <div className="flex items-center gap-1.5 text-[10px] text-red-400 font-bold bg-red-500/10 px-1.5 py-0.5 rounded uppercase tracking-wider">
                                                        <AlertCircle className="w-2.5 h-2.5" />
                                                        Error
                                                    </div>
                                                );
                                            } else if (isLoaded) {
                                                statusBadge = (
                                                    <div className="flex items-center gap-1 text-[10px] text-green-400 font-bold bg-green-500/10 px-1.5 py-0.5 rounded uppercase tracking-wider">
                                                        <CheckCircle2 className="w-2.5 h-2.5" />
                                                        Stored
                                                    </div>
                                                );
                                            }

                                            return (
                                                <div key={symbol} className={`bg-gray-900/40 border p-3 rounded-xl transition-all group flex items-center justify-between gap-4 ${isDownloadingAny ? 'border-blue-500/40 bg-blue-500/5' : 'border-gray-800/60 hover:border-blue-500/50 hover:bg-gray-900/60'
                                                    }`}>
                                                    <div className="flex items-center gap-4 min-w-0 flex-1">
                                                        <div className={`w-10 h-10 rounded-lg flex items-center justify-center border transition-colors flex-shrink-0 ${isDownloadingAny ? 'bg-blue-600/20 border-blue-500/50' : 'bg-gray-950 border-gray-800 group-hover:border-blue-500/30'
                                                            }`}>
                                                            <BarChart3 className={`w-5 h-5 ${isLoaded || isDownloadingRaw ? 'text-blue-500' : 'text-gray-600'}`} />
                                                        </div>
                                                        <div className="min-w-0">
                                                            <h3 className="text-sm font-bold text-gray-100 truncate" title={symbol}>{symbol}</h3>
                                                            <div className="flex items-center gap-2 mt-0.5">
                                                                {statusBadge}
                                                            </div>
                                                        </div>
                                                    </div>

                                                    <div className="text-center text-xs font-mono text-gray-400 truncate flex-shrink-0" style={{ width: columnWidths.first }}>
                                                        {metadata?.time_from ? formatDate(metadata.time_from) : '—'}
                                                    </div>
                                                    <div className="text-center text-xs font-mono text-gray-400 truncate flex-shrink-0" style={{ width: columnWidths.last }}>
                                                        {metadata?.time_to ? formatDate(metadata.time_to) : '—'}
                                                    </div>

                                                    <div className="flex gap-2 ml-4 flex-shrink-0" style={{ width: columnWidths.action }}>
                                                        <button
                                                            onClick={() => handleQuickDownload(symbol, 'raw')}
                                                            disabled={isDownloadingRaw || startDownload.isPending}
                                                            className={`px-3 py-2 rounded-lg text-xs font-bold transition-all flex items-center justify-center gap-2 min-w-[100px] ${isDownloadingRaw
                                                                ? 'bg-blue-600/20 text-blue-400 cursor-not-allowed border border-blue-500/30'
                                                                : isLoaded
                                                                    ? 'bg-gray-800 text-gray-400 hover:bg-blue-600 hover:text-white'
                                                                    : 'bg-blue-600 text-white hover:bg-blue-700 shadow-lg shadow-blue-500/10'
                                                                }`}
                                                            title="Download Candles"
                                                        >
                                                            {isDownloadingRaw ? <Loader2 className="w-3 h-3 animate-spin" /> : <BarChart3 className="w-3 h-3" />}
                                                            {isDownloadingRaw ? 'Busy' : 'Candles'}
                                                        </button>
                                                        {['future', 'swap', 'linear', 'derivative'].some(d => selectedMarket.toLowerCase().includes(d)) && (
                                                            <button
                                                                onClick={() => handleQuickDownload(symbol, 'funding')}
                                                                disabled={isDownloadingFunding || startDownload.isPending}
                                                                className={`px-3 py-2 rounded-lg text-xs font-bold transition-all flex items-center justify-center gap-2 min-w-[100px] ${isDownloadingFunding
                                                                    ? 'bg-purple-600/20 text-purple-400 cursor-not-allowed border border-purple-500/30'
                                                                    : 'bg-purple-600 text-white hover:bg-purple-700 shadow-lg shadow-purple-500/10'
                                                                    }`}
                                                                title="Download Funding"
                                                            >
                                                                {isDownloadingFunding ? <Loader2 className="w-3 h-3 animate-spin" /> : <DollarSign className="w-3 h-3" />}
                                                                {isDownloadingFunding ? 'Busy' : 'Funding'}
                                                            </button>
                                                        )}
                                                        <button
                                                            onClick={() => handleDeleteHistory(symbol)}
                                                            disabled={isDownloadingAny || deleteSymbol.isPending}
                                                            className="p-2 ml-2 rounded-lg text-gray-600 hover:bg-red-500/10 hover:text-red-400 border border-transparent hover:border-red-500/30 transition-all"
                                                            title="Clear History"
                                                        >
                                                            {deleteSymbol.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                                                        </button>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                )}
                            </div>
                        </div>
                    </>
                ) : (
                    <div className="flex-1 flex flex-col items-center justify-center bg-gray-950 border border-gray-800 rounded-3xl p-20 text-center">
                        <div className="w-20 h-20 bg-blue-600/10 rounded-3xl flex items-center justify-center mb-6">
                            <FileUp className="w-10 h-10 text-blue-500" />
                        </div>
                        <h4 className="text-2xl font-bold text-gray-100">Manual File Ingestion</h4>
                        <p className="text-gray-500 mt-2 max-w-sm">Upload your local CSV files and we will automatically convert them to our structured lake format.</p>
                        <button className="mt-8 px-8 py-3 bg-gray-900 border border-gray-800 text-gray-300 rounded-2xl font-bold hover:bg-gray-800 transition-all">
                            Select Source File
                        </button>
                    </div>
                )}
            </div>

            {/* Global Progress Panel (Fixed Overlay) */}
            {downloadStatus && Object.keys(downloadStatus).length > 0 && (
                <div className={`fixed bottom-6 right-6 w-80 bg-gray-900 border border-gray-800 rounded-2xl shadow-2xl overflow-hidden z-50 animate-in slide-in-from-bottom-5 transition-all duration-300 ${isCollapsed ? 'h-[46px]' : ''}`}>
                    <div
                        className="p-3 bg-gray-800/50 border-b border-gray-700 flex items-center justify-between cursor-pointer hover:bg-gray-800 transition-colors"
                        onClick={() => setIsCollapsed(!isCollapsed)}
                    >
                        <div className="flex items-center gap-2">
                            <Download className="w-4 h-4 text-blue-400" />
                            <h4 className="text-xs font-bold uppercase tracking-wider">Active Downloads</h4>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-[10px] font-bold bg-blue-600 px-1.5 py-0.5 rounded text-white">
                                {Object.values(downloadStatus).filter(t => t.status === 'running' || t.status === 'pending').length}
                            </span>
                            {isCollapsed ? <ChevronUp className="w-4 h-4 text-gray-400" /> : <ChevronDown className="w-4 h-4 text-gray-400" />}
                        </div>
                    </div>
                    {!isCollapsed && (
                        <div className="max-h-64 overflow-y-auto p-2 space-y-2 custom-scrollbar">
                            {Object.entries(downloadStatus)
                                .filter(([_, t]) => t.status === 'running' || t.status === 'pending')
                                .map(([taskKey, t]) => (
                                    <div key={taskKey} className="bg-gray-950 p-2 rounded-lg border border-gray-800 text-[11px] flex items-center justify-between">
                                        <div className="min-w-0 flex-1">
                                            <div className="flex items-center gap-2">
                                                <div className="flex flex-col">
                                                    <span className="text-[7px] font-bold text-blue-500 uppercase leading-none">{t.exchange} • {t.market}</span>
                                                    <span className="font-bold text-gray-200 truncate mt-0.5">{t.symbol}</span>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 mt-1">
                                                <span className={`text-[8px] uppercase font-bold ${t.status === 'running' ? 'text-blue-400' : 'text-yellow-400'}`}>
                                                    {t.status}
                                                </span>
                                                <p className="text-gray-500 truncate text-[10px]">{t.message}</p>
                                            </div>
                                        </div>
                                        {t.status === 'running' && <Loader2 className="w-3.5 h-3.5 animate-spin text-blue-500" />}
                                    </div>
                                ))}

                            {Object.values(downloadStatus).every(t => t.status === 'completed' || t.status === 'failed') && (
                                <div className="py-4 text-center">
                                    <p className="text-xs text-gray-500">All tasks finished</p>
                                    <button
                                        onClick={() => queryClient.invalidateQueries({ queryKey: ['download-status'] })}
                                        className="mt-2 text-[10px] text-blue-400 hover:underline"
                                    >
                                        Clear history
                                    </button>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};
