const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  allowEIO3: true,
  pingInterval: 10000, // 10초마다 핑 송신
  pingTimeout: 30000,  // 30초 동안 응답 없으면 타임아웃 처리 (AVD 연결 단절 극대화 방지)
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.get('/', (req, res) => {
    res.status(200).send('Server is running!');
});

// Unique instance ID to detect if players are hitting different server instances (e.g. during rolling deploys or scaling)
const serverInstanceId = Math.random().toString(36).substring(2, 10).toUpperCase();
console.log(`[START] 서버 인스턴스 ID: ${serverInstanceId}, 프로세스 ID: ${process.pid}`);

let lobbies = {};

io.on('connection', (socket) => {
    console.log(`[CONNECT] 클라이언트 연결됨: ${socket.id} (서버인스턴스: ${serverInstanceId})`);

    // 방 생성
    socket.on('create_room', async () => {
        const roomCode = Math.floor(100 + Math.random() * 900).toString();
        await socket.join(roomCode);
        console.log(`[CREATE_ROOM] 방 생성됨: roomCode=${roomCode}, host=${socket.id} (서버인스턴스: ${serverInstanceId})`);
        
        const response = JSON.stringify({
            roomCode: roomCode,
            playerId: socket.id,
            serverInstanceId: serverInstanceId
        });
        console.log(`[CREATE_ROOM] room_created 전송: ${response}`);
        socket.emit('room_created', response);
    });

    // 방 참가
    socket.on('join_room', async (data) => {
        console.log(`[JOIN_ROOM] join_room 수신: type=${typeof data}, data=${JSON.stringify(data).substring(0, 200)} (서버인스턴스: ${serverInstanceId})`);
        if (typeof data === 'string') { try { data = JSON.parse(data); } catch (e) { console.log(`[JOIN_ROOM] JSON parse 실패: ${e.message}`); } }
        const { roomCode } = data || {};
        
        if (!roomCode || typeof roomCode !== 'string') {
            console.log(`[JOIN_ROOM] 방코드 누락 또는 유효하지 않음: roomCode=${roomCode}`);
            socket.emit('join_error', JSON.stringify({ message: '방 코드가 유효하지 않습니다.', roomCode: roomCode }));
            return;
        }

        // 디버그용: 현재 이 서버 인스턴스에 존재하는 모든 방 목록 출력
        const allRooms = Array.from(io.sockets.adapter.rooms.keys());
        console.log(`[JOIN_ROOM] 현재 서버의 모든 방 목록: ${JSON.stringify(allRooms)} (방코드=${roomCode})`);

        // 방이 존재하지 않아도 자동으로 방에 참가 (방장 재연결 또는 첫 참가자 모두 대응)
        await socket.join(roomCode);
        console.log(`[JOIN_ROOM] 방 참가 완료: ${socket.id} -> ${roomCode}`);
        
        const room = io.sockets.adapter.rooms.get(roomCode);
        const players = room ? Array.from(room).map(id => ({ id: id })) : [{ id: socket.id }];
        console.log(`[JOIN_ROOM] players_updated 브로드캐스트: ${JSON.stringify(players)}`);
        io.to(roomCode).emit('players_updated', JSON.stringify(players));
        
        const joinResponse = JSON.stringify({
            roomCode: roomCode,
            playerId: socket.id,
            players: players,
            serverInstanceId: serverInstanceId
        });
        console.log(`[JOIN_ROOM] room_joined 전송 to ${socket.id}: ${joinResponse}`);
        socket.emit('room_joined', joinResponse);
    });

    socket.on('host_state', (state) => {
        if (typeof state === 'string') {
            try {
                state = JSON.parse(state);
            } catch (e) {
                console.log("[HOST_STATE] JSON parse error");
                return;
            }
        }
        
        if (state && state.payload) {
            try {
                const inner = typeof state.payload === 'string' ? JSON.parse(state.payload) : state.payload;
                inner.roomCode = state.roomCode;
                state = inner;
            } catch(e) {}
        }
        
        if (state && state.roomCode) {
            const newPhase = state.phase;
            const playersCount = state.players ? state.players.length : 0;
            if (socket.lastLoggedPhase !== newPhase || socket.lastLoggedPlayersCount !== playersCount) {
                console.log(`[HOST_STATE] 방장 ${socket.id} (방코드: ${state.roomCode}) 상태 변경 알림: 국면=${socket.lastLoggedPhase || '없음'} -> ${newPhase}, 인원수=${socket.lastLoggedPlayersCount || 0}명 -> ${playersCount}명`);
                socket.lastLoggedPhase = newPhase;
                socket.lastLoggedPlayersCount = playersCount;
            }
            socket.to(state.roomCode).emit('host_state_sync', JSON.stringify(state));
        }
    });

    socket.on('guest_joy_input', (data) => {
        if (typeof data === 'string') {
            try {
                data = JSON.parse(data);
            } catch (e) {}
        }
        const { roomCode, angle, power } = data || {};
        if (roomCode) {
            socket.to(roomCode).emit('guest_joy_input_relay', JSON.stringify({ guestId: socket.id, angle, power }));
        }
    });

    socket.on('guest_ready', (data) => {
        console.log(`[GUEST_READY] 수신 from ${socket.id}: type=${typeof data}, data=${JSON.stringify(data).substring(0, 200)}`);
        if (typeof data === 'string') {
            try {
                data = JSON.parse(data);
            } catch (e) {
                console.log(`[GUEST_READY] JSON parse error: ${e.message}`);
            }
        }
        const { roomCode, isReady } = data || {};
        console.log(`[GUEST_READY] roomCode=${roomCode}, isReady=${isReady}, guestId=${socket.id}`);
        if (roomCode) {
            const relayData = JSON.stringify({ guestId: socket.id, isReady });
            console.log(`[GUEST_READY] guest_ready_relay 전송 to room ${roomCode}: ${relayData}`);
            socket.to(roomCode).emit('guest_ready_relay', relayData);
        }
    });
    
    socket.on('play_again', (data) => {
        if (typeof data === 'string') { try { data = JSON.parse(data); } catch(e){} }
        const { roomCode } = data || {};
        if (roomCode) {
            io.to(roomCode).emit('play_again_triggered');
        }
    });

    socket.on('lobby_update', (data) => {
        if (typeof data === 'string') { try { data = JSON.parse(data); } catch(e){} }
        lobbies[socket.id] = data;
        io.emit('lobby_update', JSON.stringify(data));
    });

    socket.on('room_closed', (data) => {
        if (typeof data === 'string') { try { data = JSON.parse(data); } catch(e){} }
        const { roomCode } = data || {};
        console.log(`[ROOM_CLOSED] roomCode=${roomCode}, host=${socket.id}`);
        delete lobbies[socket.id];
        io.emit('room_closed', JSON.stringify(data || {}));
        if (roomCode) {
            io.to(roomCode).emit('room_kicked', JSON.stringify({}));
        }
    });

    socket.on('get_lobby_list', () => {
        io.emit('get_lobby_list');
    });

    socket.on('leave_room', (data) => {
        if (typeof data === 'string') { try { data = JSON.parse(data); } catch(e){} }
        const { roomCode } = data || {};
        console.log(`[LEAVE_ROOM] ${socket.id} leaves roomCode=${roomCode}`);
        if (roomCode) {
            socket.leave(roomCode);
            const room = io.sockets.adapter.rooms.get(roomCode);
            if (room) {
               const players = Array.from(room).map(id => ({ id: id }));
               io.to(roomCode).emit('players_updated', JSON.stringify(players));
            }
        }
    });

    socket.on('disconnecting', () => {
        console.log(`[DISCONNECTING] ${socket.id}, rooms: ${Array.from(socket.rooms)}`);
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

    socket.on('disconnect', () => {
        console.log(`[DISCONNECT] 클라이언트 연결 끊김: ${socket.id}`);
        if (lobbies[socket.id]) {
            const data = lobbies[socket.id];
            delete lobbies[socket.id];
            io.emit('room_closed', JSON.stringify({ roomCode: data.roomCode }));
            io.to(data.roomCode).emit('room_kicked', JSON.stringify({}));
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Socket.IO 릴레이 서버가 ${PORT} 포트에서 실행 중입니다. (인스턴스 ID: ${serverInstanceId})`);
});
