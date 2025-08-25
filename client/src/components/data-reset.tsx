import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Trash2, AlertTriangle } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { queryClient } from "@/lib/queryClient";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";

export default function DataReset() {
  const { toast } = useToast();

  const resetMutation = useMutation({
    mutationFn: async () => {
      const response = await fetch('/api/reset-data', {
        method: 'POST',
      });

      if (!response.ok) {
        throw new Error('Reset failed');
      }

      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Data reset completed",
        description: "All orders, sessions, and payout logs have been cleared.",
      });
      queryClient.invalidateQueries();
    },
    onError: () => {
      toast({
        title: "Reset failed",
        description: "Could not reset data. Please try again.",
        variant: "destructive",
      });
    }
  });

  return (
    <Card className="mb-6 border-red-200 bg-red-50">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-red-800">
          <Trash2 className="h-5 w-5" />
          Reset All Data
        </CardTitle>
        <CardDescription className="text-red-700">
          Clear all uploaded orders and payout history for fresh start
        </CardDescription>
      </CardHeader>
      <CardContent>
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button 
              variant="destructive"
              className="w-full"
              data-testid="button-reset-data"
            >
              <AlertTriangle className="h-4 w-4 mr-2" />
              Reset All Data
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
              <AlertDialogDescription>
                This action cannot be undone. This will permanently delete:
              </AlertDialogDescription>
              <ul className="mt-2 list-disc list-inside text-sm text-muted-foreground">
                <li>All uploaded order data</li>
                <li>All upload sessions</li>
                <li>All payout history logs</li>
              </ul>
              <p className="text-sm text-muted-foreground mt-2">
                Product prices and shipping rates will be preserved.
              </p>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction 
                onClick={() => resetMutation.mutate()}
                disabled={resetMutation.isPending}
                className="bg-red-600 hover:bg-red-700"
              >
                {resetMutation.isPending ? 'Resetting...' : 'Yes, reset data'}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>

        <div className="mt-4 p-3 bg-red-100 rounded-lg">
          <p className="text-sm text-red-700">
            <strong>Note:</strong> This will only clear order data and logs. Your product prices and shipping rates settings will remain intact.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}