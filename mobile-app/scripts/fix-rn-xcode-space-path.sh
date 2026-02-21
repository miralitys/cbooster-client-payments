#!/usr/bin/env bash
set -euo pipefail

WITH_ENV_FILE="node_modules/react-native/scripts/xcode/with-environment.sh"
HERMES_FILE="node_modules/react-native/sdks/hermes-engine/utils/replace_hermes_version.js"
DEVTOOLS_FILE="node_modules/react-native/Libraries/Core/setUpReactDevTools.js"

if [[ -f "$WITH_ENV_FILE" ]]; then
  perl -0pi -e 's/# Execute argument, if present\nif \[ -n ".*?" \]; then\n  .*?\nfi/# Execute argument, if present\nif [ -n "\$1" ]; then\n  "\$@"\nfi/s' "$WITH_ENV_FILE"
fi

if [[ -f "$HERMES_FILE" ]]; then
  perl -0pi -e 's/execSync\(`tar -xf .*? -C .*?`\);/execSync(`tar -xf "\${tarballURLPath}" -C "\${finalLocation}"`);/s' "$HERMES_FILE"
fi

if [[ -f "$DEVTOOLS_FILE" ]]; then
  perl -0pi -e "s#\n  // 3\\. Fallback to attempting to connect WS-based RDT frontend\n  const RCTNativeAppEventEmitter = require\\('\\.\\./EventEmitter/RCTNativeAppEventEmitter'\\);\\n  RCTNativeAppEventEmitter\\.addListener\\(\\n    'RCTDevMenuShown',\\n    connectToWSBasedReactDevToolsFrontend,\\n  \\);\\n  connectToWSBasedReactDevToolsFrontend\\(\\); // Try connecting once on load\\n#\n  // Disabled for this app: do not connect to local WS DevTools frontend in Debug.\\n#s" "$DEVTOOLS_FILE"
fi
