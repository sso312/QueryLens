# QueryLENs OCI 배포 Runbook

대상 인스턴스:
- Name: `instance-team9`
- Public IP: `146.56.175.190`

## 1) 사전 준비
- OCI 서버에 Docker + Docker Compose 설치
- 로컬에 SSH KEY 파일 준비: `./instance-team9.key`
- 루트 `.env` 작성

```env
FRONTEND_PORT=8000
API_PORT=80
BACKEND_URL=http://host.docker.internal:4000

OCI_HOST=146.56.175.190
OCI_USER=opc
OCI_SSH_PORT=22
OCI_REMOTE_DIR=/home/opc/querylens
KEY_FILE=./instance-team9.key
```

## 2) 로컬 backend 실행
- 로컬 머신에서 backend를 실행
- 기본 예시는 `ql-text-sql`가 `8002`에서 떠 있는 경우를 기준으로 합니다.
- 실제 로컬 backend 포트는 자유롭게 사용 가능하며, 터널 단계에서 매핑합니다.

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}" | grep ql-text-sql
```

## 3) OCI API -> 로컬 backend 연결

### 권장 A: 직접 라우팅(VPN/공인IP)
- OCI에서 로컬 backend에 직접 접근 가능하면:
  - `BACKEND_URL=http://<LOCAL_IP>:4000`

### 권장 B: SSH Reverse Tunnel
- 1회 설정(OCI): reverse tunnel을 외부 인터페이스에 바인딩 가능하게 설정

```bash
ssh -i instance-team9.key opc@146.56.175.190 \
  "echo 'GatewayPorts clientspecified' | sudo tee /etc/ssh/sshd_config.d/querylens-tunnel.conf >/dev/null && sudo systemctl restart sshd"
```

- 로컬에서 터널 실행(기본: 로컬 `8002` -> OCI `4000`):

```bash
./scripts/start-oci-tunnel.sh
```

- 로컬 backend 포트가 다르면 환경변수로 지정:

```bash
LOCAL_BACKEND_PORT=4000 ./scripts/start-oci-tunnel.sh
```

- 이 경우 OCI의 API 컨테이너는 `BACKEND_URL=http://host.docker.internal:4000` 사용

### 참고: `ssh -L` 예시
- 아래는 로컬 테스트용:

```bash
ssh -i instance-team9.key -L 4000:localhost:4000 opc@146.56.175.190
```

## 4) 최초 배포

```bash
chmod 600 ./instance-team9.key
chmod +x ./scripts/deploy-oci.sh ./scripts/check-oci.sh ./scripts/start-oci-tunnel.sh
./scripts/deploy-oci.sh
```

## 5) 재배포

### 전체(frontend + api)
```bash
./scripts/deploy-oci.sh
```

### frontend만 (OCI 서버에서)
```bash
docker compose build frontend
docker compose up -d frontend
```

### api만 (OCI 서버에서)
```bash
docker compose build api
docker compose up -d api
```

## 6) 상태 확인

```bash
./scripts/check-oci.sh
```

확인 URL:
- Frontend: `http://146.56.175.190:8000`
- API Health: `http://146.56.175.190:80/api/health`

## 7) 자주 발생 문제
- `Permission denied (publickey)`
  - `chmod 600 instance-team9.key`
  - `OCI_USER`, `OCI_HOST` 확인
- `/api/health` 실패
  - `BACKEND_URL` 값 확인
  - 로컬 backend 실행 상태 확인
  - `./scripts/start-oci-tunnel.sh` 실행/유지 여부 확인
  - OCI에서 `ss -ltnp | grep :4000` 확인
- 외부에서 접속 불가
  - OCI NSG/Security List에서 `80`, `8000` 인바운드 허용 확인
