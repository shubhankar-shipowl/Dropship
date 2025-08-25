import { useEffect, useState } from "react";
import { CheckCircle, AlertCircle, Clock } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";

interface UploadProgressProps {
  isUploading: boolean;
  uploadSuccess: {
    processedRows: number;
    cancelledRows: number;
  } | null;
  uploadError: string | null;
}

export default function UploadProgress({ isUploading, uploadSuccess, uploadError }: UploadProgressProps) {
  const [progress, setProgress] = useState(0);
  const [timeElapsed, setTimeElapsed] = useState(0);

  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (isUploading) {
      setProgress(0);
      setTimeElapsed(0);
      
      // Ultra-fast progress simulation for optimized processing
      interval = setInterval(() => {
        setProgress(prev => {
          if (prev < 60) return prev + Math.random() * 8; // Very fast initial progress
          if (prev < 85) return prev + Math.random() * 4; // Fast progress  
          if (prev < 95) return prev + Math.random() * 1; // Slower near end
          return prev; // Stop at 95% until completion
        });
        setTimeElapsed(prev => prev + 1);
      }, 600); // Even faster updates
    } else if (uploadSuccess) {
      setProgress(100);
    } else {
      setProgress(0);
      setTimeElapsed(0);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [isUploading, uploadSuccess]);

  if (!isUploading && !uploadSuccess && !uploadError) {
    return null;
  }

  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  return (
    <Card className="mb-6">
      <CardContent className="p-4">
        {isUploading && (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Clock className="h-4 w-4 text-blue-500" />
                <span className="text-sm font-medium">Processing Excel file...</span>
              </div>
              <span className="text-sm text-gray-500">{formatTime(timeElapsed)}</span>
            </div>
            
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>Progress</span>
                <span>{Math.round(progress)}%</span>
              </div>
              <Progress value={progress} className="h-2" />
            </div>
            
            <div className="text-sm text-gray-600 space-y-1">
              {progress < 20 && <p className="text-blue-600">• Reading and parsing Excel data...</p>}
              {progress >= 20 && progress < 40 && <p className="text-blue-600">• Validating column mappings...</p>}
              {progress >= 40 && progress < 95 && <p className="text-blue-600">• Ultra-fast parallel processing to database...</p>}
              {progress >= 95 && <p className="text-green-600">• Finalizing upload process...</p>}
              <p className="text-amber-600 font-medium mt-2">Please keep this page open</p>
              <p className="text-sm text-gray-500">⚡ Parallel processing with 100-record batches for maximum reliability</p>
            </div>
          </div>
        )}

        {uploadSuccess && (
          <div className="flex items-center space-x-3 text-green-700">
            <CheckCircle className="h-5 w-5" />
            <div>
              <div className="font-medium">Upload completed successfully!</div>
              <div className="text-sm">
                Processed {uploadSuccess.processedRows} orders
                {uploadSuccess.cancelledRows > 0 && `, removed ${uploadSuccess.cancelledRows} cancelled orders`}
              </div>
            </div>
          </div>
        )}

        {uploadError && (
          <div className="flex items-center space-x-3 text-red-700">
            <AlertCircle className="h-5 w-5" />
            <div>
              <div className="font-medium">Upload failed</div>
              <div className="text-sm">{uploadError}</div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}