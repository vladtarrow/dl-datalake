import { useState } from 'react';
import { X, Table as TableIcon, ChevronLeft, ChevronRight } from 'lucide-react';
import { useDatasetPreview } from './use-datasets';

interface DataPreviewModalProps {
    datasetId: string;
    onClose: () => void;
}

export const DataPreviewModal = ({ datasetId, onClose }: DataPreviewModalProps) => {
    const [page, setPage] = useState(0);
    const limit = 100;

    const { data, isLoading, error } = useDatasetPreview(datasetId, limit, page * limit);

    const totalPages = Math.ceil((data?.total_rows || 0) / limit);
    const metadata = data?.metadata;

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm">
            <div className="bg-gray-950 border border-gray-800 rounded-3xl w-full max-w-6xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden animate-in fade-in zoom-in duration-200">
                {/* Header */}
                <header className="px-8 py-6 border-b border-gray-800 flex items-center justify-between bg-gray-900/50">
                    <div className="flex items-center gap-4">
                        <div className="p-3 bg-blue-600/20 rounded-2xl">
                            <TableIcon className="w-6 h-6 text-blue-500" />
                        </div>
                        <div>
                            <div className="flex items-center gap-3 mb-1">
                                <h3 className="text-xl font-bold">{metadata?.symbol || 'Full Data View'}</h3>
                                {metadata?.market && (
                                    <span className={`text-[10px] px-2 py-0.5 rounded font-bold border ${metadata.market.includes('FUTURES')
                                            ? 'bg-orange-600/10 text-orange-500 border-orange-500/20'
                                            : 'bg-green-600/10 text-green-500 border-green-500/20'
                                        }`}>
                                        {metadata.market}
                                    </span>
                                )}
                                {metadata?.timeframe && (
                                    <span className="text-[10px] px-2 py-0.5 bg-blue-600/10 text-blue-400 rounded font-bold border border-blue-500/20">
                                        {metadata.timeframe}
                                    </span>
                                )}
                            </div>
                            <p className="text-sm text-gray-500">
                                Type: <span className="text-gray-300 uppercase font-bold">{metadata?.type}</span> •
                                Range: {page * limit + 1} - {Math.min((page + 1) * limit, data?.total_rows || 0)} of {data?.total_rows || 0} rows
                            </p>
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-gray-800 rounded-full transition-colors"
                    >
                        <X className="w-6 h-6 text-gray-400" />
                    </button>
                </header>

                {/* Content */}
                <div className="flex-1 overflow-auto p-0 scrollbar-thin scrollbar-thumb-gray-800">
                    {isLoading ? (
                        <div className="flex flex-col items-center justify-center h-96 gap-4">
                            <div className="w-12 h-12 border-4 border-blue-600 border-t-transparent rounded-full animate-spin"></div>
                            <p className="text-gray-400 font-medium">Extracting data fragment...</p>
                        </div>
                    ) : error ? (
                        <div className="p-8 text-center text-red-400">
                            Failed to load data. The file might be corrupted or inaccessible.
                        </div>
                    ) : (
                        <table className="w-full border-collapse">
                            <thead className="sticky top-0 bg-gray-900 shadow-xl z-10">
                                <tr>
                                    {data?.columns.map((col) => (
                                        <th key={col} className="px-4 py-3 text-left text-xs font-bold text-gray-400 uppercase tracking-widest border-b border-gray-800">
                                            {col}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-800 font-mono text-[10px]">
                                {data?.rows.map((row, i) => (
                                    <tr key={i} className="hover:bg-blue-600/5 transition-colors">
                                        {data.columns.map((col) => (
                                            <td key={col} className="px-4 py-1.5 text-gray-300 border-r border-gray-900/50">
                                                {renderValue(col, row[col])}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>

                {/* Footer */}
                <footer className="px-8 py-4 border-t border-gray-800 bg-gray-900/30 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setPage(p => Math.max(0, p - 1))}
                            disabled={page === 0 || isLoading}
                            className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded-xl flex items-center gap-2 text-sm font-semibold transition-colors"
                        >
                            <ChevronLeft className="w-4 h-4" />
                            Previous
                        </button>
                        <span className="text-gray-500 text-sm">Page {page + 1} of {totalPages || 1}</span>
                        <button
                            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                            disabled={page >= totalPages - 1 || isLoading}
                            className="px-4 py-2 bg-gray-800 hover:bg-gray-700 disabled:opacity-30 rounded-xl flex items-center gap-2 text-sm font-semibold transition-colors"
                        >
                            Next
                            <ChevronRight className="w-4 h-4" />
                        </button>
                    </div>
                    <button
                        onClick={onClose}
                        className="px-6 py-2 bg-gray-100 hover:bg-white rounded-xl text-gray-950 text-sm font-bold transition-all shadow-lg"
                    >
                        Close View
                    </button>
                </footer>
            </div>
        </div>
    );
};

// Helper to format values (timestamps, numbers)
const renderValue = (col: string, val: any) => {
    if (val === null || val === undefined) { return '-'; }

    // Format timestamps
    if (col.includes('ts') || col.includes('timestamp')) {
        try {
            return new Date(val).toISOString().replace('T', ' ').slice(0, 19);
        } catch { return val; }
    }

    // Format numbers
    if (typeof val === 'number') {
        if (Math.abs(val) < 0.0000001) { return val.toExponential(4); }
        return val.toLocaleString(undefined, { maximumFractionDigits: 8 });
    }

    return String(val);
};
