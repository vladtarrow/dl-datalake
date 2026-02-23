import { NavLink } from 'react-router-dom';
import type { ReactNode } from 'react';
import { Database, Download, Settings, BarChart3 } from 'lucide-react';

interface MainLayoutProps {
    children: ReactNode;
}

export const MainLayout = ({ children }: MainLayoutProps) => {
    return (
        <div className="flex h-screen w-screen bg-gray-950 text-gray-100 overflow-hidden">
            {/* Sidebar */}
            <aside className="w-[40px] border-r border-gray-800 flex flex-col bg-gray-950/50 backdrop-blur-md z-20">
                <div className="py-4 border-b border-gray-800 flex flex-col items-center justify-center">
                    <Database className="w-5 h-5 text-blue-500 shadow-[0_0_10px_-2px_theme(colors.blue.500)]" />
                </div>

                <nav className="flex-1 p-1 space-y-2 pt-6 flex flex-col items-center">
                    <NavLink
                        to="/"
                        title="Dashboard"
                        className={({ isActive }) => `flex items-center justify-center p-2 rounded-lg transition-all w-8 h-8 ${isActive
                            ? 'bg-blue-600 border border-blue-500/50 text-white shadow-[0_0_15px_-5px_theme(colors.blue.600)]'
                            : 'text-gray-500 hover:bg-gray-900 hover:text-gray-100'
                            }`}
                    >
                        <BarChart3 className="w-4 h-4" />
                    </NavLink>

                    <NavLink
                        to="/datasets"
                        title="Datasets"
                        className={({ isActive }) => `flex items-center justify-center p-2 rounded-lg transition-all w-8 h-8 ${isActive
                            ? 'bg-blue-600 border border-blue-500/50 text-white shadow-[0_0_15px_-5px_theme(colors.blue.600)]'
                            : 'text-gray-400 hover:bg-gray-900 hover:text-gray-100'
                            }`}
                    >
                        <Database className="w-4 h-4" />
                    </NavLink>

                    <NavLink
                        to="/ingestion"
                        title="Ingestion"
                        className={({ isActive }) => `flex items-center justify-center p-2 rounded-lg transition-all w-8 h-8 ${isActive
                            ? 'bg-blue-600 border border-blue-500/50 text-white shadow-[0_0_15px_-5px_theme(colors.blue.600)]'
                            : 'text-gray-400 hover:bg-gray-900 hover:text-gray-100'
                            }`}
                    >
                        <Download className="w-4 h-4" />
                    </NavLink>
                </nav>

                <div className="p-1 pb-6 border-t border-gray-800 flex flex-col items-center">
                    <button
                        title="Settings"
                        className="flex items-center justify-center p-2 w-8 h-8 rounded-lg hover:bg-gray-900 text-gray-500 transition-all"
                    >
                        <Settings className="w-4 h-4" />
                    </button>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 overflow-y-auto p-6 px-4">
                <div className="w-full">
                    {children}
                </div>
            </main>
        </div>
    );
};
