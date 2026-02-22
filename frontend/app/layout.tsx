import React from "react"
import type { Metadata } from 'next'
import { Geist, Geist_Mono } from 'next/font/google'
import { Analytics } from '@vercel/analytics/next'
import { ThemeProvider } from '@/components/theme-provider'
import { AuthProvider } from '@/components/auth-provider'
import './globals.css'

const _geist = Geist({ subsets: ["latin"] });
const _geistMono = Geist_Mono({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: 'Query LENs - NL2SQL 의료 연구 플랫폼',
  description: '자연어로 MIMIC-IV 데이터베이스를 쿼리하고 Kaplan-Meier 생존 분석을 수행하세요',
  generator: 'v0.app',
  icons: {
    icon: '/query-lens-favicon-v2.svg',
    shortcut: '/query-lens-favicon-v2.svg',
    apple: '/query-lens-logo-v2.svg',
  },
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`font-sans antialiased`}>
        <ThemeProvider defaultTheme="system">
          <AuthProvider>
            {children}
          </AuthProvider>
        </ThemeProvider>
        <Analytics />
      </body>
    </html>
  )
}
