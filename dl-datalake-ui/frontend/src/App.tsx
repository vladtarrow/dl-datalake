import { Routes, Route, useNavigate, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MainLayout } from './components/main-layout';
import { DatasetTable } from './features/datasets/dataset-table';
import { IngestionForm } from './features/ingest/ingestion-form';
import { DataPreviewModal } from './features/datasets/data-preview-modal';

const queryClient = new QueryClient();

function App() {
  const navigate = useNavigate();
  const location = useLocation();

  // Use URL search params for preview dataset
  const searchParams = new URLSearchParams(location.search);
  const previewDatasetId = searchParams.get('preview');

  return (
    <QueryClientProvider client={queryClient}>
      <MainLayout>
        <Routes>
          <Route path="/" element={<DashboardView />} />
          <Route path="/datasets" element={<DatasetsView onPreview={(id) => navigate(`?preview=${id}`)} />} />
          <Route path="/ingestion" element={<IngestionView />} />
        </Routes>

        {previewDatasetId && (
          <DataPreviewModal
            datasetId={previewDatasetId}
            onClose={() => navigate(location.pathname)}
          />
        )}
      </MainLayout>
    </QueryClientProvider>
  );
}

const DashboardView = () => (
  <div className="space-y-8">
    <header className="flex flex-col gap-2">
      <h2 className="text-3xl font-bold capitalize">Dashboard</h2>
      <p className="text-gray-400">Manage your data lake resources efficiently.</p>
    </header>
    <div className="flex flex-col items-center justify-center py-20 bg-gray-900/50 rounded-3xl border border-gray-800">
      <h3 className="text-2xl font-bold">Welcome to DL-Lake Dashboard</h3>
      <p className="text-gray-500 mt-2">Overall stats and charts will be here.</p>
    </div>
  </div>
);

const DatasetsView = ({ onPreview }: { onPreview: (id: string) => void }) => (
  <div className="space-y-8">
    <header className="flex flex-col gap-2">
      <h2 className="text-3xl font-bold capitalize">Datasets</h2>
      <p className="text-gray-400">Manage your data lake resources efficiently.</p>
    </header>
    <section className="space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-xl font-semibold">Available Datasets</h3>
        <button
          onClick={() => queryClient.invalidateQueries({ queryKey: ['datasets'] })}
          className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg font-medium transition-colors"
        >
          Refresh Data
        </button>
      </div>
      <DatasetTable onPreview={onPreview} />
    </section>
  </div>
);

const IngestionView = () => (
  <div className="space-y-8">
    <header className="flex flex-col gap-2">
      <h2 className="text-3xl font-bold capitalize">Ingestion</h2>
      <p className="text-gray-400">Manage your data lake resources efficiently.</p>
    </header>
    <section className="space-y-6">
      <h3 className="text-xl font-semibold">Data Ingestion</h3>
      <IngestionForm />
    </section>
  </div>
);

export default App;
