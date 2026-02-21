# CBooster iPhone App (Expo + Native UI)

Нативное iPhone-приложение для CBooster с отдельным мобильным интерфейсом.
Приложение работает с вашим сервером через API (`/api/records`, `/api/moderation/...`) и не использует WebView.

## 1. Установка

```bash
cd "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments/mobile-app"
npm install
cp .env.example .env
```

В `.env` укажите адрес сервера:

```bash
EXPO_PUBLIC_API_BASE_URL=https://your-app-name.onrender.com
```

## 2. Запуск в dev (Xcode + Metro)

```bash
npx expo start --dev-client
```

Откройте workspace:

```bash
open "/Users/ramisyaparov/Desktop/Project/CBooster Client Payments/mobile-app/ios/CBoosterPayments.xcworkspace"
```

Далее в Xcode: выберите симулятор или iPhone и нажмите `Run (Cmd+R)`.

Важно: в Debug режиме `localhost:8081` используется только для JS bundler (Metro), данные при этом идут на `EXPO_PUBLIC_API_BASE_URL`.

## 3. Release сборка (без Metro)

В Xcode:
1. `Product -> Scheme -> Edit Scheme...`
2. Для `Run` выберите `Build Configuration: Release`
3. `Cmd+R`

## 4. Сборка .ipa (EAS)

```bash
npm install -g eas-cli
eas login
eas build:configure
eas build --platform ios
```

## Важно

- Используйте публичный `https` сервер (Render), не `localhost`.
- При первом запуске войдите в приложении теми же учетными данными, что и в веб-дашборде (`/login`).
- После изменения `EXPO_PUBLIC_API_BASE_URL` перезапускайте Expo.
- Открывайте именно `.xcworkspace`, не `.xcodeproj`.
