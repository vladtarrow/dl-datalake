import { useState, useRef, useEffect, useMemo } from 'react';
import { Check, ChevronDown, Search, Loader2, X } from 'lucide-react';

interface Option {
    id: string;
    name: string;
}

interface SearchableSelectProps {
    options: Option[] | undefined;
    value: string;
    onChange: (value: string) => void;
    placeholder?: string;
    loading?: boolean;
    className?: string;
}

export const SearchableSelect = ({
    options = [],
    value,
    onChange,
    placeholder = "Select option...",
    loading = false,
    className = ""
}: SearchableSelectProps) => {
    const [isOpen, setIsOpen] = useState(false);
    const [search, setSearch] = useState("");
    const containerRef = useRef<HTMLDivElement>(null);

    const selectedOption = useMemo(() =>
        options.find(opt => opt.id === value),
        [options, value]
    );

    const filteredOptions = useMemo(() => {
        if (!search) return options;
        const s = search.toLowerCase();
        return options.filter(opt =>
            opt.name.toLowerCase().includes(s) || opt.id.toLowerCase().includes(s)
        );
    }, [options, search]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };
        document.addEventListener("mousedown", handleClickOutside);
        return () => document.removeEventListener("mousedown", handleClickOutside);
    }, []);

    // Reset search when opening
    useEffect(() => {
        if (isOpen) setSearch("");
    }, [isOpen]);

    return (
        <div className={`relative ${className}`} ref={containerRef}>
            <button
                type="button"
                onClick={() => !loading && setIsOpen(!isOpen)}
                className={`w-full bg-gray-800 border border-gray-700 rounded-lg p-3 text-left flex items-center justify-between focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all ${loading ? 'opacity-50 cursor-not-allowed' : 'hover:border-gray-600'
                    }`}
            >
                <span className={`block truncate ${!selectedOption ? 'text-gray-500' : 'text-gray-100 font-medium'}`}>
                    {loading ? 'Loading...' : selectedOption ? selectedOption.name : placeholder}
                </span>
                <div className="flex items-center gap-2">
                    {loading && <Loader2 className="w-4 h-4 animate-spin text-blue-500" />}
                    <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
                </div>
            </button>

            {isOpen && (
                <div className="absolute z-50 w-full mt-2 bg-gray-900 border border-gray-700 rounded-xl shadow-2xl overflow-hidden animate-in fade-in slide-in-from-top-2 duration-200">
                    <div className="p-2 border-b border-gray-800 flex items-center gap-2 bg-gray-950/50">
                        <Search className="w-4 h-4 text-gray-500 ml-2" />
                        <input
                            autoFocus
                            className="w-full bg-transparent border-none p-2 text-sm text-gray-200 placeholder-gray-600 focus:outline-none"
                            placeholder="Filter..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                        />
                        {search && (
                            <button onClick={() => setSearch("")} className="p-1 hover:bg-gray-800 rounded">
                                <X className="w-3 h-3 text-gray-500" />
                            </button>
                        )}
                    </div>

                    <ul className="max-h-60 overflow-y-auto p-1 custom-scrollbar">
                        {filteredOptions.length === 0 ? (
                            <li className="p-4 text-center text-sm text-gray-600">No results found</li>
                        ) : (
                            filteredOptions.map((opt) => (
                                <li
                                    key={opt.id}
                                    onClick={() => {
                                        onChange(opt.id);
                                        setIsOpen(false);
                                    }}
                                    className={`flex items-center justify-between px-3 py-2.5 rounded-lg cursor-pointer text-sm transition-colors ${value === opt.id
                                            ? 'bg-blue-600/20 text-blue-400'
                                            : 'text-gray-400 hover:bg-white/5 hover:text-gray-200'
                                        }`}
                                >
                                    <span className="truncate">{opt.name}</span>
                                    {value === opt.id && <Check className="w-4 h-4" />}
                                </li>
                            ))
                        )}
                    </ul>
                </div>
            )}
        </div>
    );
};
