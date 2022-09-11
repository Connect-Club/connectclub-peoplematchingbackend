export interface PGSyncConfig {
  period: number
  lastSync: number
  syncStatus: string
  errorDescription: string
  recordsSynced: number
}
