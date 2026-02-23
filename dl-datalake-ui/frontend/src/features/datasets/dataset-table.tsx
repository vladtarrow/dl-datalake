import { useState, useMemo } from 'react';
import { useDatasets } from './use-datasets';
import type { Dataset } from './use-datasets';
import api from '../../api/api-client';
import { Eye, ChevronRight, ChevronDown, Database, Landmark, FileText, BarChart3, Download } from 'lucide-react';

interface DatasetTableProps {
    onPreview: (datasetId: string) => void;
}

export const DatasetTable = ({ onPreview }: DatasetTableProps) => {
    const [expandedExchanges, setExpandedExchanges] = useState<Set<string>>(new Set(['BINANCE']));
    const [expandedSymbols, setExpandedSymbols] = useState<Set<string>>(new Set());
    const [exportingId, setExportingId] = useState<string | null>(null);

    const handleTickerExport = async (exchange: string, symbol: string, market: string) => {
        const id = `${exchange}-${symbol}-${market}`;
        setExportingId(id);
        try {
            const response = await api.get(`/export/${exchange}/${symbol}`, {
                params: { market }
            });
            const result = response.data;
            alert(`Exported ${result.rows_exported} rows to: ${result.path}`);
        } catch (err: any) {
            console.error(err);
            const message = err.response?.data?.detail || err.message;
            alert(`Export failed: ${message}`);
        } finally {
            setExportingId(null);
        }
    };

    const handleBatchExport = async (exchange: string, e: React.MouseEvent) => {
        e.stopPropagation();
        if (!confirm(`Are you sure you want to export ALL tickers for ${exchange}? This might take a while.`)) return;

        const id = `BATCH-${exchange}`;
        setExportingId(id);
        try {
            // Using POST correctly with query params as per backend definition
            // or we could change backend to accept body, but backend definition is:
            // def export_batch(exchange: str, market: str = None):
            // FastAPI usually takes these as query params if not specified as Body. 
            // Requests might send simple params as query string for POST too if configured, 
            // but let's be explicit with 'params' config in axios.
            const response = await api.post(`/export/batch`, null, {
                params: { exchange }
            });
            const result = response.data;

            let msg = `Batch Export Complete for ${exchange}\n`;
            msg += `Total: ${result.total_symbols}\n`;
            msg += `Exported: ${result.exported_count}\n`;
            if (result.failed_count > 0) msg += `Failed: ${result.failed_count}\n`;
            if (result.skipped_count > 0) msg += `Skipped: ${result.skipped_count}\n`;
            msg += `\nDirectory: ${result.export_dir}`;

            alert(msg);
        } catch (err: any) {
            console.error(err);
            const message = err.response?.data?.detail || err.message;
            alert(`Batch export failed: ${message}`);
        } finally {
            setExportingId(null);
        }
    };

    // We fetch a large number for the structured view to ensure we can group effectively
    const { data, isLoading, error } = useDatasets({ limit: 1000 });

    const groupedData = useMemo(() => {
        if (!data?.datasets) return new Map<string, Map<string, Dataset[]>>();

        const groups = new Map<string, Map<string, Dataset[]>>();

        data.datasets.forEach(d => {
            if (!groups.has(d.exchange)) groups.set(d.exchange, new Map());
            const exchangeMap = groups.get(d.exchange)!;
            // Key by both symbol and market to differentiate them at top level
            const key = `${d.symbol}:${d.market}`;
            if (!exchangeMap.has(key)) exchangeMap.set(key, []);
            exchangeMap.get(key)!.push(d);
        });

        return groups;
    }, [data]);

    const toggleExchange = (ex: string) => {
        const next = new Set(expandedExchanges);
        if (next.has(ex)) next.delete(ex); else next.add(ex);
        setExpandedExchanges(next);
    };

    const toggleSymbol = (exKey: string) => {
        const next = new Set(expandedSymbols);
        if (next.has(exKey)) next.delete(exKey); else next.add(exKey);
        setExpandedSymbols(next);
    };

    if (isLoading) return <div className="p-8 text-center text-gray-500 animate-pulse">Scanning Data Lake...</div>;
    if (error) return <div className="p-8 text-center text-red-500 bg-red-950/20 rounded-2xl border border-red-900/50">Error accessing manifest database.</div>;

    return (
        <div className="space-y-4">
            {[...groupedData.entries()].map(([exchange, symbols]) => (
                <div key={exchange} className="overflow-hidden border border-gray-800 rounded-2xl bg-gray-900/20">
                    {/* Exchange Header */}
                    <button
                        onClick={() => toggleExchange(exchange)}
                        className="w-full flex items-center justify-between p-5 bg-gray-900/60 hover:bg-gray-800 transition-colors"
                    >
                        <div className="flex items-center gap-4">
                            <div className="p-2 bg-blue-600/10 rounded-lg">
                                <Landmark className="w-5 h-5 text-blue-400" />
                            </div>
                            <h4 className="text-lg font-bold tracking-wide">{exchange}</h4>
                            <span className="text-xs bg-gray-800 text-gray-400 px-2 py-0.5 rounded-full font-mono">
                                {symbols.size} unique datasets
                            </span>
                            <button
                                onClick={(e) => handleBatchExport(exchange, e)}
                                disabled={exportingId === `BATCH-${exchange}`}
                                className={`ml-4 px-3 py-1 text-xs font-bold rounded flex items-center gap-2 transition-all ${exportingId === `BATCH-${exchange}`
                                        ? 'bg-gray-800 text-gray-500 cursor-wait'
                                        : 'bg-blue-600/20 text-blue-400 hover:bg-blue-600 hover:text-white border border-blue-500/30'
                                    }`}
                                title="Export all tickers for this exchange"
                            >
                                <Download className={`w-3 h-3 ${exportingId === `BATCH-${exchange}` ? 'animate-bounce' : ''}`} />
                                {exportingId === `BATCH-${exchange}` ? 'Exporting...' : 'Export All'}
                            </button>
                        </div>
                        {expandedExchanges.has(exchange) ? <ChevronDown className="w-5 h-5 text-gray-500" /> : <ChevronRight className="w-5 h-5 text-gray-500" />}
                    </button>

                    {/* Symbols List */}
                    {expandedExchanges.has(exchange) && (
                        <div className="divide-y divide-gray-800/50 border-t border-gray-800">
                            {[...symbols.entries()].map(([key, files]) => {
                                const [symbol, market] = key.split(':');
                                const exKey = `${exchange}-${key}`;
                                const isExpanded = expandedSymbols.has(exKey);

                                return (
                                    <div key={key} className="bg-gray-950/20">
                                        <button
                                            onClick={() => toggleSymbol(exKey)}
                                            className="w-full flex items-center justify-between px-8 py-3 hover:bg-white/5 transition-colors"
                                        >
                                            <div className="flex items-center gap-3">
                                                <div className={`p-1 rounded ${isExpanded ? 'text-blue-500' : 'text-gray-600'}`}>
                                                    {isExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                                                </div>
                                                <span className="font-bold text-gray-200">{symbol}</span>
                                                <span className={`text-[10px] px-2 py-0.5 rounded font-bold border ${market.includes('FUTURES')
                                                    ? 'bg-orange-600/10 text-orange-500 border-orange-500/20'
                                                    : 'bg-green-600/10 text-green-500 border-green-500/20'
                                                    }`}>
                                                    {market}
                                                </span>
                                                <span className="text-[10px] text-gray-600 uppercase tracking-tighter">
                                                    {files.length} versions
                                                </span>
                                            </div>
                                            <div className="flex items-center gap-2">
                                                <button
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        handleTickerExport(exchange, symbol, market);
                                                    }}
                                                    disabled={exportingId === exKey}
                                                    className={`p-1.5 ${exportingId === exKey ? 'bg-gray-800 text-gray-600' : 'bg-green-600/10 text-green-400 hover:bg-green-600 hover:text-white'} rounded transition-all`}
                                                    title="Export 1m Aggregate CSV"
                                                >
                                                    <Download className={`w-3.5 h-3.5 ${exportingId === exKey ? 'animate-bounce' : ''}`} />
                                                </button>
                                            </div>
                                        </button>

                                        {/* Files Detail */}
                                        {isExpanded && (
                                            <div className="px-12 pb-4 pt-1 space-y-2">
                                                <table className="w-full text-left">
                                                    <thead>
                                                        <tr className="text-[10px] text-gray-600 uppercase font-bold tracking-widest border-b border-gray-800/50">
                                                            <th className="pb-2">Type</th>
                                                            <th className="pb-2">Timeframe</th>
                                                            <th className="pb-2">Date Range</th>
                                                            <th className="pb-2">Size</th>
                                                            <th className="pb-2">Last Sync</th>
                                                            <th className="pb-2 text-right">Actions</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody className="divide-y divide-gray-800/30">
                                                        {files.map(file => (
                                                            <tr key={file.id} className="group transition-colors">
                                                                <td className="py-2.5">
                                                                    <div className="flex items-center gap-2">
                                                                        {file.data_type === 'raw' ? (
                                                                            <BarChart3 className="w-3 h-3 text-blue-500" />
                                                                        ) : (
                                                                            <FileText className="w-3 h-3 text-purple-500" />
                                                                        )}
                                                                        <span className={`text-xs font-bold uppercase ${file.data_type === 'raw' ? 'text-blue-400' : 'text-purple-400'}`}>
                                                                            {file.data_type === 'raw' ? 'Candles' : file.data_type === 'alt' ? 'Funding' : file.data_type}
                                                                        </span>
                                                                    </div>
                                                                </td>
                                                                <td className="py-2.5">
                                                                    {file.timeframe ? (
                                                                        <span className="text-[10px] px-2 py-0.5 bg-blue-600/10 text-blue-400 rounded-lg font-bold border border-blue-500/20">
                                                                            {file.timeframe}
                                                                        </span>
                                                                    ) : (
                                                                        <span className="text-gray-800 text-[10px]">-</span>
                                                                    )}
                                                                </td>
                                                                <td className="py-2.5">
                                                                    {file.time_from ? (
                                                                        <div className="text-[10px] flex gap-2 items-center">
                                                                            <span className="text-gray-400">{new Date(file.time_from).toLocaleDateString()}</span>
                                                                            <span className="text-gray-700">→</span>
                                                                            <span className="text-gray-400">{new Date(file.time_to || '').toLocaleDateString()}</span>
                                                                        </div>
                                                                    ) : <span className="text-gray-800 text-xs">-</span>}
                                                                </td>
                                                                <td className="py-2.5 text-xs text-gray-500 font-mono">
                                                                    {(file.file_size_bytes / 1024 / 1024).toFixed(2)} MB
                                                                </td>
                                                                <td className="py-2.5 text-[10px] text-gray-600 font-mono italic">
                                                                    {new Date(file.last_modified).toLocaleDateString()}
                                                                </td>
                                                                <td className="py-2.5 text-right">
                                                                    <button
                                                                        onClick={() => onPreview(file.id)}
                                                                        className="p-1.5 bg-blue-600/10 text-blue-400 hover:bg-blue-600 hover:text-white rounded transition-all"
                                                                        title="Preview Data"
                                                                    >
                                                                        <Eye className="w-3.5 h-3.5" />
                                                                    </button>
                                                                </td>
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>
            ))}

            {groupedData.size === 0 && (
                <div className="flex flex-col items-center justify-center py-20 bg-gray-900/30 rounded-3xl border border-dashed border-gray-800">
                    <Database className="w-12 h-12 text-gray-800 mb-4" />
                    <p className="text-gray-600 font-semibold italic">Lake is empty. Go to Ingestion to download data.</p>
                </div>
            )}
        </div>
    );
};
