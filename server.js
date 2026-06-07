const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

let lobbies = {};

io.on('connection', (socket) => {
    console.log('클라이언트 연결됨:', socket.id);

    // 방 생성
    socket.on('create_room', () => {
        const roomCode = Math.floor(100 + Math.random() * 900).toString();
        socket.join(roomCode);
        console.log(`방 생성됨: ${roomCode} (호스트: ${socket.id})`);
        
        socket.emit('room_created', {
            roomCode: roomCode,
            playerId: socket.id
        });
    });

    // 방 참가
    socket.on('join_room', (data) => {
        const { roomCode } = data || {};
        const room = io.sockets.adapter.rooms.get(roomCode);
        
        if (room && typeof roomCode === 'string') {
            socket.join(roomCode);
            console.log(`방 참가: ${socket.id} -> ${roomCode}`);
            
            // 기존 플레이어들에게 업데이트 알림
            const players = Array.from(io.sockets.adapter.rooms.get(roomCode)).map(id => ({ id: id }));
            io.to(roomCode).emit('players_updated', players);
            
            // 참가한 플레이어에게 방 정보 전송
            socket.emit('room_joined', {
                roomCode: roomCode,
                playerId: socket.id,
                players: players
            });
        } else {
            console.log(`방 참가 실패: ${roomCode}`);
            socket.emit('error', '방을 찾을 수 없습니다.');
        }
    });

    // 방장 상태 (게임 데이터 전체 동기화)
    socket.on('host_state', (data) => {
        const { roomCode, state } = data;
        if (roomCode) {
            socket.to(roomCode).emit('host_state_sync', state);
        }
    });

    // 게스트의 조이스틱 입력 (호스트에게 전달)
    socket.on('guest_joy_input', (data) => {
        const { roomCode, angle, power } = data;
        if (roomCode) {
            // guestId는 서버에서 소켓 id로 추가해서 전달 가능 (또는 클라이언트가 보낸 값 사용 가능)
            socket.to(roomCode).emit('guest_joy_input_relay', { guestId: socket.id, angle, power });
        }
    });
    
    // 다시하기
    socket.on('play_again', (data) => {
        const { roomCode } = data;
        if (roomCode) {
            io.to(roomCode).emit('play_again_triggered');
        }
    });

    // 로비 업데이트 (호스트가 방정보 브로드캐스트)
    socket.on('lobby_update', (data) => {
        lobbies[socket.id] = data; // store by host id
        io.emit('lobby_update', data);
    });

    // 방 닫힘
    socket.on('room_closed', (data) => {
        const { roomCode } = data;
        delete lobbies[socket.id];
        io.emit('room_closed', data);
    });

    // 로비 리스트 요청
    socket.on('get_lobby_list', () => {
        io.emit('get_lobby_list');
    });

    // 방 나가기
    socket.on('leave_room', (data) => {
        const { roomCode } = data;
        if (roomCode) {
            socket.leave(roomCode);
            const room = io.sockets.adapter.rooms.get(roomCode);
            if (room) {
               const players = Array.from(room).map(id => ({ id: id }));
               io.to(roomCode).emit('players_updated', players);
            }
        }
    });

    socket.on('disconnect', () => {
        console.log('클라이언트 연결 끊김:', socket.id);
        if (lobbies[socket.id]) {
            const data = lobbies[socket.id];
            delete lobbies[socket.id];
            io.emit('room_closed', { roomCode: data.roomCode });
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Socket.IO 릴레이 서버가 ${PORT} 포트에서 실행 중입니다.`);
});
