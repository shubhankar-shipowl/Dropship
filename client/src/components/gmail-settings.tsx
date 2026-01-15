import { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import {
  Mail,
  CheckCircle2,
  XCircle,
  RefreshCw,
  ExternalLink,
  Shield,
  AlertTriangle,
  Loader2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { useToast } from "@/hooks/use-toast";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";

interface GmailStatus {
  success: boolean;
  status: {
    credentialsLoaded: boolean;
    authorized: boolean;
    redirectUri: string;
    scopes: string[];
    tokenPath: string;
    credentialsPath: string;
    userEmail: string | null;
    messagesTotal: number;
  };
}


export default function GmailSettings() {
  const { toast } = useToast();
  const [isAuthorizing, setIsAuthorizing] = useState(false);

  // Fetch Gmail status
  const {
    data: gmailStatus,
    isLoading: isLoadingStatus,
    refetch: refetchStatus,
    error: statusError,
  } = useQuery<GmailStatus>({
    queryKey: ["/api/gmail/status"],
    refetchInterval: isAuthorizing ? 3000 : false, // Poll while authorizing
  });


  // Revoke access mutation
  const revokeMutation = useMutation({
    mutationFn: async () => {
      const response = await fetch("/api/gmail/revoke", { method: "POST" });
      if (!response.ok) throw new Error("Failed to revoke access");
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Gmail Access Revoked",
        description: "You can re-authorize anytime.",
      });
      refetchStatus();
    },
    onError: () => {
      toast({
        title: "Error",
        description: "Failed to revoke Gmail access",
        variant: "destructive",
      });
    },
  });

  // Handle authorization - opens Google consent in new window
  const handleAuthorize = () => {
    setIsAuthorizing(true);

    // Open authorization in a popup window
    const width = 600;
    const height = 700;
    const left = window.screenX + (window.outerWidth - width) / 2;
    const top = window.screenY + (window.outerHeight - height) / 2;

    const authWindow = window.open(
      "/api/gmail/authorize?redirect=true",
      "Gmail Authorization",
      `width=${width},height=${height},left=${left},top=${top},toolbar=no,menubar=no,scrollbars=yes,resizable=yes`
    );

    // Poll to check if window is closed
    const checkClosed = setInterval(() => {
      if (authWindow?.closed) {
        clearInterval(checkClosed);
        setIsAuthorizing(false);
        // Refetch status after authorization window closes
        setTimeout(() => {
          refetchStatus();
        }, 1000);
      }
    }, 500);

    // Safety timeout
    setTimeout(() => {
      clearInterval(checkClosed);
      setIsAuthorizing(false);
      refetchStatus();
    }, 300000); // 5 minutes max
  };

  // Listen for postMessage from OAuth callback (alternative approach)
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      if (event.data?.type === "gmail-oauth-success") {
        setIsAuthorizing(false);
        refetchStatus();
        toast({
          title: "Gmail Connected!",
          description: `Successfully connected to ${event.data.email}`,
        });
      }
    };

    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, [refetchStatus, toast]);

  const isAuthorized = gmailStatus?.status?.authorized;
  const userEmail = gmailStatus?.status?.userEmail;

  if (isLoadingStatus) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      {/* Status Card */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div
                className={`p-2 rounded-lg ${
                  isAuthorized
                    ? "bg-green-100 dark:bg-green-900/30"
                    : "bg-amber-100 dark:bg-amber-900/30"
                }`}
              >
                <Mail
                  className={`h-5 w-5 ${
                    isAuthorized
                      ? "text-green-600 dark:text-green-400"
                      : "text-amber-600 dark:text-amber-400"
                  }`}
                />
              </div>
              <div>
                <CardTitle className="text-lg">Gmail API Integration</CardTitle>
                <CardDescription>
                  Send emails with labels using Gmail API
                </CardDescription>
              </div>
            </div>
            <Badge
              variant={isAuthorized ? "default" : "secondary"}
              className={
                isAuthorized
                  ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                  : ""
              }
            >
              {isAuthorized ? (
                <>
                  <CheckCircle2 className="h-3 w-3 mr-1" />
                  Connected
                </>
              ) : (
                <>
                  <XCircle className="h-3 w-3 mr-1" />
                  Not Connected
                </>
              )}
            </Badge>
          </div>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* Credentials Status */}
          <div className="flex items-center gap-2 text-sm">
            {gmailStatus?.status?.credentialsLoaded ? (
              <>
                <CheckCircle2 className="h-4 w-4 text-green-500" />
                <span className="text-muted-foreground">
                  Credentials loaded from <code className="text-xs bg-muted px-1 py-0.5 rounded">cred.json</code>
                </span>
              </>
            ) : (
              <>
                <XCircle className="h-4 w-4 text-red-500" />
                <span className="text-muted-foreground">
                  Missing <code className="text-xs bg-muted px-1 py-0.5 rounded">cred.json</code>
                </span>
              </>
            )}
          </div>

          {/* Connected Account */}
          {isAuthorized && userEmail && (
            <div className="flex items-center gap-2 text-sm">
              <Shield className="h-4 w-4 text-blue-500" />
              <span className="text-muted-foreground">Connected as:</span>
              <Badge variant="outline" className="font-mono text-xs">
                {userEmail}
              </Badge>
            </div>
          )}

          <Separator />

          {/* Action Buttons */}
          <div className="flex flex-wrap gap-3">
            {!isAuthorized ? (
              <Button
                onClick={handleAuthorize}
                disabled={isAuthorizing || !gmailStatus?.status?.credentialsLoaded}
                className="gap-2"
              >
                {isAuthorizing ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Authorizing...
                  </>
                ) : (
                  <>
                    <ExternalLink className="h-4 w-4" />
                    Authorize Gmail Access
                  </>
                )}
              </Button>
            ) : (
              <>
                <Button
                  variant="outline"
                  onClick={() => refetchStatus()}
                  className="gap-2"
                >
                  <RefreshCw className="h-4 w-4" />
                  Refresh Status
                </Button>
                <Button
                  variant="destructive"
                  onClick={() => revokeMutation.mutate()}
                  disabled={revokeMutation.isPending}
                  className="gap-2"
                >
                  {revokeMutation.isPending ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <XCircle className="h-4 w-4" />
                  )}
                  Revoke Access
                </Button>
              </>
            )}
          </div>

          {/* Info Alert for non-authorized state */}
          {!isAuthorized && gmailStatus?.status?.credentialsLoaded && (
            <Alert>
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Authorization Required</AlertTitle>
              <AlertDescription>
                Click "Authorize Gmail Access" to connect your Google account.
                A popup window will open for you to sign in and grant permissions.
                After authorization, you can send emails with labels applied automatically.
              </AlertDescription>
            </Alert>
          )}

          {/* Missing credentials alert */}
          {!gmailStatus?.status?.credentialsLoaded && (
            <Alert variant="destructive">
              <XCircle className="h-4 w-4" />
              <AlertTitle>Missing Credentials</AlertTitle>
              <AlertDescription>
                The <code>cred.json</code> file is missing from the root directory.
                Please add your Google Cloud OAuth2 credentials to enable Gmail API.
              </AlertDescription>
            </Alert>
          )}
        </CardContent>
      </Card>


    </div>
  );
}
