# 소개팅 게임 (Sogaeting Game) 릴레이 서버

이 디렉토리는 소개팅 게임의 온라인 멀티플레이 기능을 위한 **Node.js + Socket.IO 중계 서버**입니다.
현재 안드로이드 프로젝트 내에서는 자체적으로 서버를 구동(호스팅)할 수 없기 때문에,
P2P 모의 연결이 아닌 실제 온라인 연결을 하시려면 이 폴더의 코드를 실제 클라우드 환경에 배포해주셔야 합니다.

## 배포 방법 (Render, Heroku, Glitch 등)

1. 로컬 환경 혹은 클라우드에 이 `server` 폴더 코드(`package.json`, `server.js`)를 업로드합니다.
2. 패키지를 설치합니다: `npm install`
3. 서버를 실행합니다: `npm start`
4. 서버의 배포된 실제 URL 주소를 안드로이드 앱의 `GameViewModel.kt` 에 있는 `_serverUrl` 에 적용합니다.

```kotlin
// app/src/main/java/com/example/game/GameViewModel.kt
private val _serverUrl = MutableStateFlow("https://여러분의-클라우드-서버-주소.com") 
```

서버 연결이 성공하면 앱에서 방 생성 시 'P2P 모의 네트워크'가 아닌 실제 **'온라인 매치(방 생성됨)'** 로 동작하게 됩니다.
