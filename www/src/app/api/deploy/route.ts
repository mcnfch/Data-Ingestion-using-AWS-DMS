import { NextRequest, NextResponse } from 'next/server'
import { spawn } from 'child_process'
import path from 'path'

export async function POST(request: NextRequest) {
  const encoder = new TextEncoder()
  
  const stream = new ReadableStream({
    start(controller) {
      const binPath = path.join(process.cwd(), '../bin')
      const deployProcess = spawn('python3', ['deploy.py'], {
        cwd: binPath,
        env: { ...process.env }
      })
      
      deployProcess.stdout.on('data', (data) => {
        controller.enqueue(encoder.encode(`data: ${data.toString()}\n\n`))
      })
      
      deployProcess.stderr.on('data', (data) => {
        controller.enqueue(encoder.encode(`data: ERROR: ${data.toString()}\n\n`))
      })
      
      deployProcess.on('close', (code) => {
        controller.enqueue(encoder.encode(`data: Process completed with code ${code}\n\n`))
        controller.close()
      })
      
      deployProcess.on('error', (error) => {
        controller.enqueue(encoder.encode(`data: ERROR: ${error.message}\n\n`))
        controller.close()
      })
    }
  })
  
  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    },
  })
}