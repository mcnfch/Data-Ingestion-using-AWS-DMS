import { NextRequest, NextResponse } from 'next/server'
import { exec } from 'child_process'
import { promisify } from 'util'
import path from 'path'

const execAsync = promisify(exec)

export async function POST(request: NextRequest) {
  try {
    const binPath = path.join(process.cwd(), '../bin')
    const command = `cd "${binPath}" && echo "DESTROY" | python3 unwind.py`
    
    console.log('Executing infrastructure unwind:', command)
    
    const { stdout, stderr } = await execAsync(command, {
      timeout: 2400000, // 40 minutes timeout
      env: { ...process.env }
    })
    
    console.log('Unwind stdout:', stdout)
    if (stderr) console.error('Unwind stderr:', stderr)
    
    return NextResponse.json({
      success: true,
      stdout,
      stderr: stderr || null
    })
    
  } catch (error: any) {
    console.error('Unwind error:', error)
    
    return NextResponse.json({
      success: false,
      error: error.message,
      stdout: error.stdout || '',
      stderr: error.stderr || ''
    }, { status: 500 })
  }
}