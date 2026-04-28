# Load environment variables
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#=]+)=(.+)$') {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($key, $value, 'Process')
    }
}

$ZHIPU_KEY = $env:ZHIPU_API_KEY
$MINIMAX_KEY_1 = $env:MINIMAX_API_KEY_1
$GUIDOR_KEY = $env:GUIDOR_API_KEY
$GUIDOR_GPT_KEY = $env:GUIDOR_GPT_API_KEY
$GUIDOR_DEEPSEEK_KEY = $env:GUIDOR_DEEPSEEK_API_KEY

$results = @()

# Test function
function Test-Model {
    param($name, $url, $key, $model, $protocol)
    
    Write-Host "Testing $name ($model)..." -ForegroundColor Cyan
    
    if ($protocol -eq "openai") {
        $body = @{
            model = $model
            messages = @(@{role="user"; content="hi"})
            max_tokens = 10
        } | ConvertTo-Json -Depth 10
        
        $headers = @{
            "Authorization" = "Bearer $key"
            "Content-Type" = "application/json"
        }
        
        $endpoint = "$url/v1/chat/completions"
    } else {
        # Anthropic protocol
        $body = @{
            model = $model
            messages = @(@{role="user"; content="hi"})
            max_tokens = 10
        } | ConvertTo-Json -Depth 10
        
        $headers = @{
            "x-api-key" = $key
            "anthropic-version" = "2023-06-01"
            "Content-Type" = "application/json"
        }
        
        $endpoint = "$url/v1/messages"
    }
    
    try {
        $response = Invoke-WebRequest -Uri $endpoint -Method Post -Headers $headers -Body $body -TimeoutSec 30 -ErrorAction Stop
        $status = "✓ OK"
        $color = "Green"
    } catch {
        $status = "✗ FAIL: $($_.Exception.Message)"
        $color = "Red"
    }
    
    Write-Host "  $status" -ForegroundColor $color
    
    return [PSCustomObject]@{
        Backend = $name
        Model = $model
        Protocol = $protocol
        Status = $status
    }
}

# Test GLM models (OpenAI)
$results += Test-Model "zhipu-openai" "https://open.bigmodel.cn/api/coding/paas" $ZHIPU_KEY "glm-5.1" "openai"
$results += Test-Model "zhipu-openai" "https://open.bigmodel.cn/api/coding/paas" $ZHIPU_KEY "glm-5v-turbo" "openai"
$results += Test-Model "zhipu-openai" "https://open.bigmodel.cn/api/coding/paas" $ZHIPU_KEY "glm-5-turbo" "openai"
$results += Test-Model "zhipu-openai" "https://open.bigmodel.cn/api/coding/paas" $ZHIPU_KEY "glm-4.7" "openai"

# Test MiniMax models (OpenAI)
$results += Test-Model "minimax-coding-1" "https://api.minimaxi.com" $MINIMAX_KEY_1 "MiniMax-M2.7" "openai"
$results += Test-Model "minimax-coding-1" "https://api.minimaxi.com" $MINIMAX_KEY_1 "MiniMax-M2.7-highspeed" "openai"

# Test Claude models (OpenAI via Guidor)
$results += Test-Model "guidor" "https://guidor.vip" $GUIDOR_KEY "[REDACTED]" "openai"
$results += Test-Model "guidor" "https://guidor.vip" $GUIDOR_KEY "[REDACTED]" "openai"
$results += Test-Model "guidor" "https://guidor.vip" $GUIDOR_KEY "[REDACTED]" "openai"

# Test GPT models (OpenAI via Guidor)
$results += Test-Model "guidor-gpt" "https://guidor.vip" $GUIDOR_GPT_KEY "gpt-5.5" "openai"
$results += Test-Model "guidor-gpt" "https://guidor.vip" $GUIDOR_GPT_KEY "gpt-5.4" "openai"

# Test DeepSeek models (OpenAI via Guidor)
$results += Test-Model "guidor-deepseek" "https://guidor.vip" $GUIDOR_DEEPSEEK_KEY "deepseek-v4-pro" "openai"
$results += Test-Model "guidor-deepseek" "https://guidor.vip" $GUIDOR_DEEPSEEK_KEY "deepseek-v4-flash" "openai"

Write-Host "`n=== Summary ===" -ForegroundColor Yellow
$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -notlike "*OK*" }
if ($failed) {
    Write-Host "`nFailed models:" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  - $($_.Model) ($($_.Backend))" -ForegroundColor Red }
} else {
    Write-Host "`nAll models passed!" -ForegroundColor Green
}
