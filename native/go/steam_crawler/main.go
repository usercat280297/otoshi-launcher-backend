package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type steamApp struct {
	AppID int    `json:"appid"`
	Name  string `json:"name"`
}

type steamAppListResponse struct {
	AppList struct {
		Apps []steamApp `json:"apps"`
	} `json:"applist"`
}

type crawlerOutput struct {
	Source    string                   `json:"source"`
	FetchedAt string                   `json:"fetched_at"`
	Count     int                      `json:"count"`
	Items     []map[string]interface{} `json:"items"`
}

func main() {
	url := flag.String("url", "https://api.steampowered.com/ISteamApps/GetAppList/v2/", "Steam app list URL")
	limit := flag.Int("limit", 5000, "Maximum items to emit")
	timeoutSec := flag.Int("timeout", 20, "HTTP timeout in seconds")
	flag.Parse()

	client := &http.Client{Timeout: time.Duration(*timeoutSec) * time.Second}
	resp, err := client.Get(*url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "steam crawler request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "steam crawler unexpected status: %d\n", resp.StatusCode)
		os.Exit(1)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "steam crawler read failed: %v\n", err)
		os.Exit(1)
	}

	var payload steamAppListResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		fmt.Fprintf(os.Stderr, "steam crawler decode failed: %v\n", err)
		os.Exit(1)
	}

	maxCount := *limit
	if maxCount <= 0 || maxCount > len(payload.AppList.Apps) {
		maxCount = len(payload.AppList.Apps)
	}

	items := make([]map[string]interface{}, 0, maxCount)
	for _, app := range payload.AppList.Apps {
		if len(items) >= maxCount {
			break
		}
		if app.AppID <= 0 || app.Name == "" {
			continue
		}
		items = append(items, map[string]interface{}{
			"app_id": fmt.Sprintf("%d", app.AppID),
			"name":   app.Name,
		})
	}

	out := crawlerOutput{
		Source:    "steam_api",
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		Count:     len(items),
		Items:     items,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "steam crawler write failed: %v\n", err)
		os.Exit(1)
	}
}
