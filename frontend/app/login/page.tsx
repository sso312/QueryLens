"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { Shield, LogIn, UserPlus } from "lucide-react"
import { useAuth } from "@/components/auth-provider"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"

type AuthMode = "login" | "signup"

export default function LoginPage() {
  const router = useRouter()
  const { user, isHydrated, login, register } = useAuth()

  const [mode, setMode] = useState<AuthMode>("login")
  const [username, setUsername] = useState("researcher_01")
  const [password, setPassword] = useState("team9KDT__2026")
  const [name, setName] = useState("")
  const [role, setRole] = useState("연구원")
  const [department, setDepartment] = useState("")
  const [passwordConfirm, setPasswordConfirm] = useState("")

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  useEffect(() => {
    if (!isHydrated) return
    if (user) {
      router.replace("/")
    }
  }, [isHydrated, user, router])

  const resetMessages = () => {
    setError(null)
    setSuccess(null)
  }

  const handleLogin = async () => {
    const result = await login({ username, password })
    if (!result.ok) {
      setError(result.error || "로그인에 실패했습니다.")
      return
    }
    router.replace("/")
  }

  const handleSignup = async () => {
    if (password !== passwordConfirm) {
      setError("비밀번호 확인이 일치하지 않습니다.")
      return
    }
    const reg = await register({ username, password, name, role, department })
    if (!reg.ok) {
      setError(reg.error || "회원가입에 실패했습니다.")
      return
    }
    setSuccess("회원가입이 완료되었습니다. 로그인 처리 중입니다.")
    router.replace("/")
  }

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    resetMessages()
    setSubmitting(true)
    try {
      if (mode === "login") {
        await handleLogin()
      } else {
        await handleSignup()
      }
    } finally {
      setSubmitting(false)
    }
  }

  if (!isHydrated) {
    return (
      <div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">
        로그인 상태를 확인 중입니다...
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <Card className="w-full max-w-md">
        <CardHeader>
          <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center mb-2">
            <Shield className="w-5 h-5 text-primary" />
          </div>
          <CardTitle>Query LENs {mode === "login" ? "로그인" : "회원가입"}</CardTitle>
          <CardDescription>
            {mode === "login"
              ? "상단 사용자 프로필과 연동되는 계정으로 로그인하세요."
              : "새 계정을 만들고 바로 로그인할 수 있습니다."}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-4 grid grid-cols-2 gap-2">
            <Button
              type="button"
              variant={mode === "login" ? "default" : "outline"}
              onClick={() => {
                setMode("login")
                resetMessages()
              }}
            >
              로그인
            </Button>
            <Button
              type="button"
              variant={mode === "signup" ? "default" : "outline"}
              onClick={() => {
                setMode("signup")
                resetMessages()
              }}
            >
              회원가입
            </Button>
          </div>

          <form className="space-y-4" onSubmit={handleSubmit}>
            {mode === "signup" ? (
              <div className="space-y-1.5">
                <Label htmlFor="name">이름</Label>
                <Input
                  id="name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="홍길동"
                  autoComplete="name"
                  required
                />
              </div>
            ) : null}

            <div className="space-y-1.5">
              <Label htmlFor="username">아이디</Label>
              <Input
                id="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="researcher_01"
                autoComplete="username"
                required
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="password">비밀번호</Label>
              <Input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="********"
                autoComplete={mode === "login" ? "current-password" : "new-password"}
                required
              />
            </div>

            {mode === "signup" ? (
              <>
                <div className="space-y-1.5">
                  <Label htmlFor="passwordConfirm">비밀번호 확인</Label>
                  <Input
                    id="passwordConfirm"
                    type="password"
                    value={passwordConfirm}
                    onChange={(e) => setPasswordConfirm(e.target.value)}
                    placeholder="********"
                    autoComplete="new-password"
                    required
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="role">역할</Label>
                  <Input
                    id="role"
                    value={role}
                    onChange={(e) => setRole(e.target.value)}
                    placeholder="연구원"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="department">부서</Label>
                  <Input
                    id="department"
                    value={department}
                    onChange={(e) => setDepartment(e.target.value)}
                    placeholder="임상연구팀"
                    required
                  />
                </div>
              </>
            ) : null}

            {error ? <div className="text-sm text-destructive">{error}</div> : null}
            {success ? <div className="text-sm text-primary">{success}</div> : null}

            <Button type="submit" className="w-full gap-2" disabled={submitting}>
              {mode === "login" ? <LogIn className="w-4 h-4" /> : <UserPlus className="w-4 h-4" />}
              {submitting ? (mode === "login" ? "로그인 중..." : "가입 중...") : (mode === "login" ? "로그인" : "회원가입")}
            </Button>
          </form>

          {mode === "signup" ? (
            <div className="mt-4 text-xs text-muted-foreground">
              회원가입 후 자동으로 로그인됩니다.
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  )
}
