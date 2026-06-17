const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Socket.IO Server configuration with CORS and optimized timeouts
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingInterval: 600000, // 10 minutes (prevents disconnects when emulators freeze during boot)
  pingTimeout: 300000,  // 5 minutes
  allowEIO3: true       // Allow Socket.IO client v3 connections
});

// Engine connection error listener
io.engine.on("connection_error", (err) => {
  console.log(`[ENGINE_CONN_ERR] 연결 에러 발생 - IP: ${err.req ? err.req.socket.remoteAddress : 'unknown'}, 코드: ${err.code}, 메시지: ${err.message}`);
});

// Root endpoint check
app.get('/', (req, res) => {
  res.status(200).send('Server is running!');
});

// Server Instance ID to distinguish between multiple servers or reboots
const serverInstanceId = Math.random().toString(36).substring(2, 10).toUpperCase();
console.log(`[START] 서버가 시작되었습니다. 인스턴스 ID: ${serverInstanceId}, 프로세스 ID: ${process.pid}`);

// Lobbies cache: roomCode -> lobby metadata
let lobbies = {};
// Reconnection timers for hosts: roomCode -> setTimeout handle
const hostReconnectionTimers = {};

io.on('connection', (socket) => {
  const ua = socket.handshake.headers['user-agent'] || 'unknown';
  const transport = socket.conn.transport.name;
  console.log(`[CONNECT] 클라이언트 연결됨 - 소켓ID: ${socket.id}, IP: ${socket.handshake.address}, Transport: ${transport}, UA: ${ua}`);

  // 모든 인입 패킷 로깅용 미들웨어
  socket.use((packet, next) => {
    const eventName = packet[0];
    let dataStr = "";
    try {
      dataStr = typeof packet[1] === 'object' ? JSON.stringify(packet[1]) : String(packet[1] || '');
      if (dataStr.length > 500) {
        dataStr = dataStr.substring(0, 500) + "... (truncated)";
      }
    } catch (e) {
      dataStr = "[Unparseable]";
    }
    console.log(`[RECV_EVENT] 소켓ID: ${socket.id}, UUID: ${socket.uuid || 'N/A'}, 이벤트: "${eventName}", 데이터: ${dataStr}`);
    next();
  });

  socket.on('error', (err) => {
    console.error(`[SOCKET_ERROR] 소켓ID: ${socket.id}, 에러: ${err.message || err}`);
  });

  // 1. 방 생성 (Host)
  socket.on('create_room', async (data) => {
    let uuid;
    if (data) {
      if (typeof data === 'string') {
        try { data = JSON.parse(data); } catch (e) { }
      }
      uuid = data.uuid;
      if (uuid) {
        socket.uuid = uuid;
        console.log(`  - [CREATE_ROOM] 소켓 ${socket.id} 에 UUID 바인딩: ${socket.uuid}`);
      }
    }

    // 3자리 방 코드 생성 (100 ~ 999)
    let roomCode;
    do {
      roomCode = Math.floor(100 + Math.random() * 900).toString();
    } while (lobbies[roomCode] || io.sockets.adapter.rooms.has(roomCode));

    await socket.join(roomCode);
    console.log(`[CREATE_ROOM] 방 생성 완료. 코드: #${roomCode}, 방장소켓: ${socket.id}, 방장UUID: ${socket.uuid || 'N/A'}`);

    // 로비 목록에 선등록하여 유령 방 방지 및 UUID 기반 클렌징 대상에 즉시 포함되도록 함
    lobbies[roomCode] = {
      roomCode: roomCode,
      roomName: '대기실',
      isPrivate: false,
      playersCount: 1,
      leadEmoji: '🐶',
      hostSocketId: socket.id,
      hostUuid: socket.uuid || uuid || 'N/A'
    };
    console.log(`  - [CREATE_ROOM] 방 #${roomCode} lobbies 맵 선등록 완료 (UUID: ${lobbies[roomCode].hostUuid})`);

    const response = JSON.stringify({
      roomCode: roomCode,
      playerId: socket.id,
      serverInstanceId: serverInstanceId
    });
    socket.emit('room_created', response);
  });

  // 2. 방 참가 (Guest or Reconnecting Host)
  socket.on('join_room', async (data) => {
    console.log(`[JOIN_ROOM] join_room 수신 from ${socket.id}: ${JSON.stringify(data)}`);
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch (e) {
        console.error(`[JOIN_ROOM] JSON 파싱 에러: ${e.message}`);
      }
    }

    const { roomCode, isHost } = data || {};

    if (!roomCode) {
      socket.emit('join_error', JSON.stringify({ message: '방 코드가 유효하지 않습니다.', roomCode: '' }));
      return;
    }

    // 방이 존재하는지 확인 (Socket.IO adapter 기준 또는 로비 캐시 기준)
    const roomExists = io.sockets.adapter.rooms.has(roomCode) || lobbies[roomCode];
    if (!roomExists) {
      console.log(`[JOIN_ROOM] 참가 실패: 존재하지 않는 방 #${roomCode}`);
      socket.emit('join_error', JSON.stringify({ message: '존재하지 않는 방입니다.', roomCode: roomCode }));
      return;
    }

    // 해당 방에 대한 재연결 타이머가 진행 중이었다면 해제
    if (hostReconnectionTimers[roomCode]) {
      clearTimeout(hostReconnectionTimers[roomCode]);
      delete hostReconnectionTimers[roomCode];
      console.log(`[RECONNECT] 방장 재연결 감지 -> #${roomCode} 자동 삭제 타이머 취소`);
    }

    // 방장 재접속 시 호스트 소켓 정보 업데이트
    if (isHost && lobbies[roomCode]) {
      console.log(`[RECONNECT] 방장 소켓 정보 변경: #${roomCode}, 옛 소켓: ${lobbies[roomCode].hostSocketId} -> 새 소켓: ${socket.id}`);
      lobbies[roomCode].hostSocketId = socket.id;
    }

    // 소켓 방 조인
    await socket.join(roomCode);
    console.log(`[JOIN_ROOM] 소켓 조인 성공: ${socket.id} -> #${roomCode}`);

    // 현재 방에 접속 중인 플레이어 목록 가져오기
    const room = io.sockets.adapter.rooms.get(roomCode);
    const players = room ? Array.from(room).map(id => ({ id: id })) : [{ id: socket.id }];

    // 방 전체에 최신 인원 정보 업데이트
    const playersUpdatedStr = JSON.stringify(players);
    console.log(`[JOIN_ROOM] players_updated 브로드캐스트: #${roomCode}, 인원: ${players.length}명`);
    io.to(roomCode).emit('players_updated', playersUpdatedStr);

    // 참가자 본인에게 성공 통지
    const joinResponse = JSON.stringify({
      roomCode: roomCode,
      playerId: socket.id,
      players: players,
      serverInstanceId: serverInstanceId
    });
    socket.emit('room_joined', joinResponse);
  });

  // 3. 방장 상태 동기화 (Host -> Guests)
  socket.on('host_state', (state) => {
    let parsedState = state;
    if (typeof state === 'string') {
      try {
        parsedState = JSON.parse(state);
      } catch (e) {
        return;
      }
    }

    // payload로 래핑되어 있는 경우 언래핑
    if (parsedState && parsedState.payload) {
      try {
        const inner = typeof parsedState.payload === 'string' ? JSON.parse(parsedState.payload) : parsedState.payload;
        inner.roomCode = parsedState.roomCode;
        parsedState = inner;
      } catch (e) { }
    }

    if (parsedState && parsedState.roomCode) {
      // 보낸 호스트 소켓 본인을 제외하고 같은 방 게스트들에게 브로드캐스트
      socket.to(parsedState.roomCode).emit('host_state_sync', JSON.stringify(parsedState));
    }
  });

  // 4. 게스트 조이스틱 입력 전달 (Guest -> Host/Room)
  socket.on('guest_joy_input', (data) => {
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch (e) { }
    }
    const { roomCode, angle, power } = data || {};
    if (roomCode) {
      socket.to(roomCode).emit('guest_joy_input_relay', JSON.stringify({
        guestId: socket.id,
        angle: angle,
        power: power
      }));
    }
  });

  // 5. 게스트 준비 상태 및 재접속/직접퇴장 중계
  socket.on('guest_ready', (data) => {
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch (e) { }
    }

    const { roomCode, isReady, isReconnect, oldPlayerId, isExplicitExit } = data || {};
    if (!roomCode) return;

    if (isReconnect) {
      console.log(`[GUEST_RECONNECT] 게스트 재접속 알림: #${roomCode}, ${oldPlayerId} -> ${socket.id}`);
      socket.to(roomCode).emit('guest_ready_relay', JSON.stringify({
        guestId: socket.id,
        oldPlayerId: oldPlayerId,
        isReconnect: true
      }));
    } else if (isExplicitExit) {
      console.log(`[GUEST_EXIT] 게스트 직접 퇴장: #${roomCode}, ${socket.id}`);
      socket.to(roomCode).emit('guest_ready_relay', JSON.stringify({
        guestId: socket.id,
        isExplicitExit: true
      }));
    } else {
      console.log(`[GUEST_READY] 준비 상태 전송: #${roomCode}, 게스트: ${socket.id}, 상태: ${isReady}, 닉네임: ${data.nickname}, 이모지: ${data.emoji}`);
      socket.to(roomCode).emit('guest_ready_relay', JSON.stringify({
        guestId: socket.id,
        isReady: isReady,
        nickname: data.nickname,
        emoji: data.emoji,
        isSpectator: data.isSpectator === true
      }));
    }
  });

  // 6. 게임 다시 시작 (Host)
  socket.on('play_again', (data) => {
    if (typeof data === 'string') {
      try { data = JSON.parse(data); } catch (e) { }
    }
    const { roomCode } = data || {};
    if (roomCode) {
      io.to(roomCode).emit('play_again_triggered');
    }
  });

  // 7. 로비 상태 업데이트 (Host)
  socket.on('lobby_update', (data) => {
    if (typeof data === 'string') {
      try { data = JSON.parse(data); } catch (e) { }
    }
    console.log(`[LOBBY_UPDATE_REC] 수신 from 소켓: ${socket.id}, UUID: ${socket.uuid}, 데이터: ${JSON.stringify(data)}`);
    if (data && data.roomCode) {
      if (data.hostUuid) {
        socket.uuid = data.hostUuid; // Bind UUID to the socket session dynamically
        console.log(`  - 소켓 ${socket.id} 에 UUID 동적 바인딩 성공: ${socket.uuid}`);
      }
      lobbies[data.roomCode] = {
        roomCode: data.roomCode,
        roomName: data.roomName || '소개팅 대결방',
        isPrivate: data.isPrivate === true,
        playersCount: data.playersCount || 1,
        leadEmoji: data.leadEmoji || '🐶',
        hostSocketId: socket.id,
        hostUuid: data.hostUuid || socket.uuid, // Save host player's hardware UUID from payload or socket context
        maxPlayers: data.maxPlayers || 4,
        isStarted: data.isStarted === true
      };
      console.log(`[LOBBY_UPDATE_SAVE] 저장완료 -> 방 #${data.roomCode}, 방장소켓: ${socket.id}, 방장UUID: ${lobbies[data.roomCode].hostUuid}`);
      // 모든 대기실 메뉴의 유저들에게 업데이트된 로비 상태 브로드캐스트
      io.emit('lobby_update', JSON.stringify(lobbies[data.roomCode]));
    }
  });

  // 8. 로비 목록 요청
  socket.on('get_lobby_list', () => {
    console.log(`[GET_LOBBY_LIST] 요청 수신 from 소켓: ${socket.id}, UUID: ${socket.uuid}. 현재 lobbies 목록 크기: ${Object.keys(lobbies).length}개`);
    // 1) 캐시되어 있는 공개 방 목록을 즉시 개별 송신
    Object.values(lobbies).forEach((lobby) => {
      if (!lobby.isPrivate) {
        console.log(`  - 개별 송신 반환: 방 #${lobby.roomCode}, 방장소켓: ${lobby.hostSocketId}, 방장UUID: ${lobby.hostUuid}`);
        socket.emit('lobby_update', JSON.stringify(lobby));
      }
    });

    // 2) 호스트들에게 최신 상태 갱신을 트리거하기 위해 브로드캐스트
    console.log(`  - 호스트 최신화 트리거 브로드캐스트 전송 (io.emit('get_lobby_list'))`);
    io.emit('get_lobby_list');
  });

  // 8-2. 플레이어 고유 기기 UUID 등록 (핫 리스타트 및 클렌징 목적)
  socket.on('register_uuid', (uuid) => {
    if (!uuid) {
      console.log(`[REGISTER_UUID] 경고: 빈 UUID 전달받음 from 소켓: ${socket.id}`);
      return;
    }
    console.log(`[REGISTER_UUID] 요청 수신 from 소켓: ${socket.id}, 등록할 UUID: ${uuid}`);

    // 중복 제거: 동일한 UUID를 가진 다른 구 소켓 연결이 살아있다면 즉시 강제 종료(좀비 예방)
    const socketMap = io.of("/").sockets;
    console.log(`  - 현재 서버 전체 활성 소켓 개수: ${socketMapMapSize(socketMap)}개. 중복 제거 루프 시작...`);
    for (const [id, s] of socketMap) {
      console.log(`    * 검사 소켓: ${s.id}, 바인딩된 s.uuid: ${s.uuid}`);
      if (s.id !== socket.id && s.uuid === uuid) {
        console.log(`    => [DUPLICATE FOUND] 동일 UUID의 구 좀비 소켓 강제 종료 실행: ${s.id}`);
        s.disconnect(true);
      }
    }

    socket.uuid = uuid;
    console.log(`[REGISTER_UUID] 소켓 ${socket.id} 에 UUID 등록 완료: ${uuid}`);

    // 혹시 이 UUID를 방장으로 등록하고 있던 활성/대기 방이 있다면 즉시 파괴
    for (const [roomCode, lobby] of Object.entries(lobbies)) {
      console.log(`  - 유령방 검사: 방 #${roomCode}, 방장UUID: ${lobby.hostUuid}, 방장소켓: ${lobby.hostSocketId}`);
      if (lobby.hostUuid === uuid) {
        console.log(`  => [CLEANUP TARGET] 매칭 성공! 구 유령 방 #${roomCode} 즉시 폭파 결정`);

        // 이전 방장 좀비 소켓 강제 해제 실행하여 확실히 숨통을 끊음 (A-1 소켓 제거)
        const oldHostSocket = io.sockets.sockets.get(lobby.hostSocketId);
        if (oldHostSocket) {
          console.log(`    => [CLEANUP] 이전 방장의 구 좀비 소켓 강제 disconnect 실행: ${oldHostSocket.id}`);
          oldHostSocket.disconnect(true);
        } else {
          console.log(`    => [CLEANUP] 이전 방장의 구 좀비 소켓 (${lobby.hostSocketId})이 이미 서버 소켓 목록에 없습니다.`);
        }

        if (hostReconnectionTimers[roomCode]) {
          clearTimeout(hostReconnectionTimers[roomCode]);
          delete hostReconnectionTimers[roomCode];
          console.log(`    => [CLEANUP] #${roomCode}의 재연결 타이머 제거 완료`);
        }

        delete lobbies[roomCode];
        io.to(roomCode).emit('room_kicked');
        io.emit('room_closed', JSON.stringify({ roomCode: roomCode }));
        console.log(`    => [CLEANUP] #${roomCode} lobbies 맵에서 삭제 및 룸 게스트 킥 전송 완료`);
      }
    }
  });

  // 9. 방 강제 종료 (Host)
  socket.on('room_closed', (data) => {
    if (typeof data === 'string') {
      try { data = JSON.parse(data); } catch (e) { }
    }
    const { roomCode } = data || {};
    if (roomCode) {
      console.log(`[ROOM_CLOSED] 방장 직접 방 종료: #${roomCode}`);
      delete lobbies[roomCode];
      if (hostReconnectionTimers[roomCode]) {
        clearTimeout(hostReconnectionTimers[roomCode]);
        delete hostReconnectionTimers[roomCode];
      }
      io.to(roomCode).emit('room_kicked');
      io.emit('room_closed', JSON.stringify({ roomCode: roomCode }));
    }
  });

  // 10. 방 나가기 (Client)
  socket.on('leave_room', (data) => {
    if (typeof data === 'string') {
      try { data = JSON.parse(data); } catch (e) { }
    }
    const { roomCode } = data || {};
    if (roomCode) {
      console.log(`[LEAVE_ROOM] 클라이언트 직접 방 나감: ${socket.id} -> #${roomCode}`);
      socket.leave(roomCode);

      const room = io.sockets.adapter.rooms.get(roomCode);
      if (room) {
        const players = Array.from(room).map(id => ({ id: id }));
        io.to(roomCode).emit('players_updated', JSON.stringify(players));
      }
    }
  });

  // 11. 연결 해제 중
  socket.on('disconnecting', () => {
    console.log(`[DISCONNECTING] 연결 종료 중: ${socket.id}, UUID: ${socket.uuid || 'N/A'}`);
    for (const roomCode of socket.rooms) {
      if (roomCode !== socket.id) {
        const room = io.sockets.adapter.rooms.get(roomCode);
        if (room) {
          const players = Array.from(room)
            .filter(id => id !== socket.id)
            .map(id => ({ id: id }));
          socket.to(roomCode).emit('players_updated', JSON.stringify(players));
        }
      }
    }
  });

  // 12. 연결 해제 완료
  socket.on('disconnect', (reason) => {
    console.log(`[DISCONNECT] 연결 종료 완료: ${socket.id}, 사유: ${reason}, UUID: ${socket.uuid || 'N/A'}`);

    // 혹시 이 소켓이 방장이었던 방이 있는지 검색
    for (const [roomCode, lobby] of Object.entries(lobbies)) {
      if (lobby.hostSocketId === socket.id) {
        console.log(`[DISCONNECT] 방장 소켓 끊김 감지: #${roomCode}. 10초 재연결 대기...`);

        if (hostReconnectionTimers[roomCode]) {
          clearTimeout(hostReconnectionTimers[roomCode]);
          delete hostReconnectionTimers[roomCode];
        }

        // 10초의 유예 기간 제공 (방장의 일시적 인터넷 끊김이나 에뮬레이터 재시동 지원)
        hostReconnectionTimers[roomCode] = setTimeout(() => {
          console.log(`[TIMEOUT] 방장 재연결 시간 만료: 방 #${roomCode} 정리`);
          delete lobbies[roomCode];
          io.to(roomCode).emit('room_kicked');
          io.emit('room_closed', JSON.stringify({ roomCode: roomCode }));
          delete hostReconnectionTimers[roomCode];
        }, 10000);
      }
    }
  });
});

// Helper for socket map size
function socketMapMapSize(socketMap) {
  return socketMap.size || Object.keys(socketMap).length || 0;
}

// 주기적 유령 방 클렌징 타이머 (5초마다 실행하여 더욱 빠르게 대응)
setInterval(() => {
  const activeLobbyCount = Object.keys(lobbies).length;
  if (activeLobbyCount > 0) {
    console.log(`[GC] 활성 룸 스캔 시작... (총 룸 개수: ${activeLobbyCount}개)`);
  }
  for (const [roomCode, lobby] of Object.entries(lobbies)) {
    const hostSocket = io.sockets.sockets.get(lobby.hostSocketId);
    const isHostAlive = hostSocket && hostSocket.connected;
    const socketUuid = hostSocket ? hostSocket.uuid : 'N/A';
    const isUuidMatch = hostSocket ? (hostSocket.uuid === lobby.hostUuid) : false;

    console.log(`  - GC 검사: 방 #${roomCode}, 방장소켓: ${lobby.hostSocketId}, 실제존재여부: ${!!hostSocket}, 연결상태: ${isHostAlive}, UUID일치여부: ${isUuidMatch} (소켓UUID: ${socketUuid} vs 방장UUID: ${lobby.hostUuid})`);

    // 만약 방장 소켓이 유실되었거나 연결 끊긴 상태이거나, 혹은 살아있지만 UUID가 일치하지 않는 경우
    if (!isHostAlive || !isUuidMatch) {
      // 이미 10초 재연결 타이머가 설정되어 대기 중인지 확인
      if (!hostReconnectionTimers[roomCode]) {
        console.log(`  => [GC CLEANUP TARGET] 방장 소켓 문제 감지 (소켓유실/연결끊김/UUID불일치) 및 타임아웃 미등록 -> 방 #${roomCode} 즉시 폭파`);
        delete lobbies[roomCode];
        io.to(roomCode).emit('room_kicked');
        io.emit('room_closed', JSON.stringify({ roomCode: roomCode }));
      } else {
        console.log(`  => [GC] 방장 소켓에 문제가 있으나 현재 10초 재연결 대기 타이머 진행 중`);
      }
    }
  }
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Socket.IO 릴레이 서버가 0.0.0.0:${PORT}에서 정상 실행 중입니다. (인스턴스 ID: ${serverInstanceId})`);
});
