'use client'

import { useState, useEffect, useRef } from 'react'

interface DeploymentStatus {
  infra: 'idle' | 'running' | 'completed' | 'failed'
  db_init: 'idle' | 'running' | 'completed' | 'failed'
  dms: 'idle' | 'running' | 'completed' | 'failed'
  validate: 'idle' | 'running' | 'completed' | 'failed'
}

interface DeploymentSummary {
  completed: number
  failed: number
  skipped: number
  pending: number
  duration: string
}

const statusIcons = {
  idle: '⏭️',
  running: '⏳',
  completed: '✅',
  failed: '❌'
}

export default function DeploymentControlPanel() {
  const [deploymentStatus, setDeploymentStatus] = useState<DeploymentStatus>({
    infra: 'idle',
    db_init: 'idle',
    dms: 'idle',
    validate: 'idle'
  })
  
  const [summary, setSummary] = useState<DeploymentSummary>({
    completed: 0,
    failed: 0,
    skipped: 0,
    pending: 5,
    duration: '0s'
  })
  
  const [logs, setLogs] = useState<string[]>([])
  const [isDeploying, setIsDeploying] = useState(false)
  const logContainerRef = useRef<HTMLDivElement>(null)

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
    }
  }, [logs])

  const addLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString('en-US', { hour12: false })
    setLogs(prev => [...prev, `[${timestamp}] ${message}`])
  }

  const updateStatus = (step: keyof DeploymentStatus, status: 'running' | 'completed' | 'failed') => {
    setDeploymentStatus(prev => {
      const oldStatus = prev[step]
      if (oldStatus === status) return prev // No change needed
      
      const newStatus = { ...prev, [step]: status }
      
      // Update summary only when status actually changes
      setSummary(prevSummary => {
        const newSummary = { ...prevSummary }
        
        // Subtract old status
        if (oldStatus === 'completed') newSummary.completed -= 1
        else if (oldStatus === 'failed') newSummary.failed -= 1
        else if (oldStatus === 'idle') newSummary.pending -= 1
        
        // Add new status  
        if (status === 'completed') newSummary.completed += 1
        else if (status === 'failed') newSummary.failed += 1
        else if (status === 'running') newSummary.pending = Math.max(0, newSummary.pending)
        
        return newSummary
      })
      
      return newStatus
    })
  }

  const startDeployment = async () => {
    if (isDeploying) return
    
    setIsDeploying(true)
    setLogs([])
    
    // Reset all status
    setDeploymentStatus({
      infra: 'idle',
      db_init: 'idle',
      dms: 'idle',
      validate: 'idle'
    })
    
    setSummary({
      completed: 0,
      failed: 0,
      skipped: 0,
      pending: 4,
      duration: '0s'
    })

    const startTime = Date.now()
    
    try {
      addLog('🚀 Starting AWS DMS deployment...')
      
      const response = await fetch('/api/deploy', { method: 'POST' })
      const reader = response.body?.getReader()
      
      if (!reader) throw new Error('Failed to get response stream')
      
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        
        const chunk = new TextDecoder().decode(value)
        const lines = chunk.split('\n').filter(line => line.startsWith('data: '))
        
        for (const line of lines) {
          const data = line.replace('data: ', '').trim()
          if (!data) continue
          
          addLog(data)
          
          // Parse logs to update status
          if (data.includes('Infrastructure')) {
            if (data.includes('starting') || data.includes('Creating') || data.includes('RUNNING')) {
              updateStatus('infra', 'running')
            } else if (data.includes('completed') || data.includes('skipping') || data.includes('SUCCESS') || data.includes('✅')) {
              updateStatus('infra', 'completed')
            } else if (data.includes('FAILED') || data.includes('ERROR') || data.includes('❌')) {
              updateStatus('infra', 'failed')
            }
          } else if (data.includes('Database initialization')) {
            if (data.includes('starting') || data.includes('Creating') || data.includes('RUNNING')) {
              updateStatus('db_init', 'running')
            } else if (data.includes('completed') || data.includes('skipping') || data.includes('SUCCESS') || data.includes('✅')) {
              updateStatus('db_init', 'completed')
            } else if (data.includes('FAILED') || data.includes('ERROR') || data.includes('❌')) {
              updateStatus('db_init', 'failed')
            }
          } else if (data.includes('DMS migration')) {
            if (data.includes('starting') || data.includes('Creating') || data.includes('RUNNING')) {
              updateStatus('dms', 'running')
            } else if (data.includes('completed') || data.includes('skipping') || data.includes('SUCCESS') || data.includes('✅')) {
              updateStatus('dms', 'completed')
            } else if (data.includes('FAILED') || data.includes('ERROR') || data.includes('❌')) {
              updateStatus('dms', 'failed')
            }
          } else if (data.includes('validation')) {
            if (data.includes('Starting validation') || data.includes('starting') || data.includes('setup')) {
              updateStatus('validate', 'running')
            } else if (data.includes('Validation and monitoring completed') || data.includes('Data validation successful') || data.includes('completed') || data.includes('SUCCESS') || data.includes('✅')) {
              updateStatus('validate', 'completed')
            } else if (data.includes('FAILED') || data.includes('ERROR') || data.includes('❌')) {
              updateStatus('validate', 'failed')
            }
          }
        }
      }
      
      const duration = Math.round((Date.now() - startTime) / 1000)
      setSummary(prev => ({ ...prev, duration: `${Math.floor(duration / 60)}m ${duration % 60}s` }))
      
    } catch (error) {
      addLog(`💥 Deployment failed: ${error}`)
    } finally {
      setIsDeploying(false)
    }
  }

  const startUnwind = async () => {
    if (isDeploying) return
    
    setIsDeploying(true)
    addLog('🧹 Starting infrastructure unwind...')
    
    const startTime = Date.now()
    
    try {
      // Reset all statuses to idle during unwind
      setDeploymentStatus({
        infra: 'idle',
        db_init: 'idle', 
        dms: 'idle',
        validate: 'idle'
      })
      
      // Reset summary
      setSummary({
        completed: 0,
        failed: 0,
        skipped: 0,
        pending: 4,
        duration: '0s'
      })
      
      // Simulate reverse order teardown
      addLog('🗑️  Removing validation and monitoring...')
      updateStatus('validate', 'running')
      await new Promise(resolve => setTimeout(resolve, 1000))
      updateStatus('validate', 'completed')
      addLog('✅ Validation resources removed')
      
      addLog('🗑️  Tearing down DMS migration...')
      updateStatus('dms', 'running')
      await new Promise(resolve => setTimeout(resolve, 1000))
      updateStatus('dms', 'completed')
      addLog('✅ DMS resources removed')
      
      addLog('🗑️  Cleaning up database...')
      updateStatus('db_init', 'running')
      await new Promise(resolve => setTimeout(resolve, 1000))
      updateStatus('db_init', 'completed')
      addLog('✅ Database resources removed')
      
      addLog('🗑️  Removing infrastructure...')
      updateStatus('infra', 'running')
      
      const response = await fetch('/api/deploy/unwind', { method: 'POST' })
      if (response.ok) {
        updateStatus('infra', 'completed')
        addLog('✅ Infrastructure unwound successfully')
        
        const duration = Math.round((Date.now() - startTime) / 1000)
        setSummary(prev => ({ ...prev, duration: `${Math.floor(duration / 60)}m ${duration % 60}s` }))
        addLog('🎉 Complete infrastructure teardown finished!')
        
        // Reset to clean state after successful unwind
        setTimeout(() => {
          setDeploymentStatus({
            infra: 'idle',
            db_init: 'idle',
            dms: 'idle', 
            validate: 'idle'
          })
          setSummary({
            completed: 0,
            failed: 0,
            skipped: 0,
            pending: 4,
            duration: '0s'
          })
        }, 2000)
        
      } else {
        updateStatus('infra', 'failed')
        addLog('❌ Unwind failed')
      }
    } catch (error) {
      updateStatus('infra', 'failed')
      addLog(`💥 Unwind error: ${error}`)
    } finally {
      setIsDeploying(false)
    }
  }

  return (
    <div className="min-h-screen bg-black text-green-400 font-mono p-4">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-2xl mb-4 text-center">AWS DMS Deployment Control Panel</h1>
        
        {/* Upper Panel - Control and Status */}
        <div className="border border-green-400 rounded p-4 mb-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
          
          {/* Left Side - Deployment Steps */}
          <div className="border border-green-400 rounded p-4">
            <h2 className="text-lg mb-3">Deployment Steps</h2>
            <div className="space-y-2">
              {Object.entries(deploymentStatus).map(([step, status]) => (
                <div key={step} className="flex items-center justify-between">
                  <span className="capitalize">{step.replace('_', ' ')}</span>
                  <span className="text-xl">{statusIcons[status as keyof typeof statusIcons]}</span>
                </div>
              ))}
            </div>
            
            <div className="mt-4 space-y-2">
              <button
                onClick={startDeployment}
                disabled={isDeploying}
                className="w-full bg-green-800 hover:bg-green-700 disabled:bg-gray-600 text-white py-2 px-4 rounded transition-colors"
              >
                {isDeploying ? '⏳ Deploying...' : '🚀 Start Deployment'}
              </button>
              
              <button
                onClick={startUnwind}
                disabled={isDeploying}
                className="w-full bg-red-800 hover:bg-red-700 disabled:bg-gray-600 text-white py-2 px-4 rounded transition-colors"
              >
                {isDeploying ? '⏳ Processing...' : '🧹 Unwind Infrastructure'}
              </button>
            </div>
          </div>
          
          {/* Right Side - Deployment Summary */}
          <div className="border border-green-400 rounded p-4">
            <h2 className="text-lg mb-3">Deployment Summary</h2>
            <div className="space-y-1">
              <div className="border-b border-green-400 pb-2 mb-2">
                <div className="text-center text-sm">DEPLOYMENT SUMMARY</div>
              </div>
              <div>✅ Completed: {summary.completed}</div>
              <div>❌ Failed: {summary.failed}</div>
              <div>⏭️ Skipped: {summary.skipped}</div>
              <div>⏳ Pending: {summary.pending}</div>
              <div>⏱️ Total Duration: {summary.duration}</div>
            </div>
          </div>
        </div>
        
        {/* Lower Panel - Live Logs */}
        <div className="border border-green-400 rounded p-4 h-96">
          <h2 className="text-lg mb-3">Deployment Logs</h2>
          <div 
            ref={logContainerRef}
            className="bg-black border border-green-400 rounded p-2 h-80 overflow-y-auto text-sm"
          >
            {logs.length === 0 ? (
              <div className="text-gray-500">Waiting for deployment to start...</div>
            ) : (
              logs.map((log, index) => (
                <div key={index} className="mb-1">
                  {log}
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  )
}