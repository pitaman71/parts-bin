#!/usr/bin/python

import arrow
import datetime
import functools
import inspect
import json
import traceback
import secrets

class Call:
    def __init__(self, stack_entry, func=None):
        self.func = func
        self.stack_entry = stack_entry
        self.purpose = None
        self.filename = None
        self.lineno = None

    def inspect(self):
        caller = inspect.getframeinfo(self.stack_entry)
        self.filename = caller.filename.split('/')[-1]
        self.lineno = caller.lineno
        self.purpose = '%s.%d: ' % (self.filename,caller.lineno)
        if hasattr(self.func, 'im_class'):
            self.purpose += self.func.im_class.__name__
            self.purpose += '.'
        elif hasattr(self.func, '__qualname__'):
            self.purpose += self.func.__qualname__
        elif hasattr(self.func, '__name__'):
            self.purpose += self.func.__name__

    def get_filename(self):
        if self.filename is None:
            self.inspect()
        return self.filename

    def get_lineno(self):
        if self.lineno is None:
            self.inspect()
        return self.lineno

    def __str__(self):
        if self.purpose is None:
            self.inspect()
        return self.purpose

def function_task(*deco_args,**deco_kwargs):
    """Decorate a function as a trackable task"""
    def wrap_function_task(func):
        @functools.wraps(func)
        def call_function_task(*call_args,**call_kwargs):
            purpose = Call(inspect.currentframe().f_back, func)
            with Task(purpose,*deco_args,**deco_kwargs).arguments(*call_args,**call_kwargs) as top_task:
                result = func(*call_args,**call_kwargs) 
                top_task.returns(result)
            return result
        return call_function_task
    return wrap_function_task

class Task:
    def __init__(self,purpose=None,logMethod=None,traceMethod=None,saveStack=False,parent=None):
        self.parent = parent
        self.parent_id = None
        self.id_ = None
        self.purpose = purpose
        self.startTime = datetime.datetime.now()
        self.endTime = None
        self.unitsExpected = dict()
        self.unitsConsumed = dict()
        self.logMethod = logMethod
        self.traceMethod = traceMethod

        self.warnings = []
        self.errors = []
        self.args = None
        self.kwargs = None
        self.returnValue = None
        self.exception = None

    def __enter__(self):
        self.id_ = secrets.randbits(31)
        self.startTime = datetime.datetime.now()
        self.status = 'BEGIN'
        self.doLog()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.endTime = datetime.datetime.now()
        if exc_type is not None:
            self.status = 'FAIL '
            self.exception = { 'exc_type': exc_type, 'exc_value': exc_value, 'traceback': traceback }
            self._write_operands()
        else:
            self.status = 'END  '
        self.doLog()
        if exc_type is not None:
            self.doLog(message=str(traceback))
        self.status = 'DONE'

    def _write_operands(self):
        if self.args is not None:
            with open(f"task{self.id_}.args.json", 'wt') as fp:
                fp.write(json.dumps(self.args, default=str))
            with open(f"task{self.id_}.kwargs.json", 'wt') as fp:
                fp.write(json.dumps(self.kwargs, default=str))

    def doLog(self, message=None):
        combined = str(self) if message is None else f'{str(self)} {str(message)}'
        #if self.traceMethod is not None:
        #    self.traceMethod(self)
        if self.logMethod == True:
            print(combined)
        elif self.logMethod == False or self.logMethod is None:
            pass
        elif hasattr(self.logMethod, 'write'):
            self.logMethod.write('%s\n' % combined)
        elif hasattr(self.logMethod, '__call__'):
            self.logMethod(combined)
        else:
            raise RuntimeError('Misconfigured Task: logMethod should be a file-like output stream, a callable, True, False, or None')

    def expectUnits(self,unitType,unitCount):
        self.unitsExpected[unitType] = unitCount
        self.unitsConsumed[unitType] = 0

    def consumeUnits(self,unitType,unitCount):
        self.unitsConsumed[unitType] += unitCount

    def arguments(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def returns(self,value):
        self.returnValue = value
        return value

    def info(self,message):
        asString = '\n'.join(message) if isinstance(message,list) else str(message)
        self.doLog(asString)

    def warning(self,message):
        asString = '\n'.join(message) if isinstance(message,list) else str(message)
        asList   = message if isinstance(message,list) else [str(message)]
        self.doLog(asString)
        self.warnings += asList

    def error(self,message):
        asString = '\n'.join(message) if isinstance(message,list) else str(message)
        asList   = message if isinstance(message,list) else [str(message)]
        self.doLog(asString)
        self.errors += asList

    def hasErrors(self):
        return self.errors is not None and len(self.errors) > 0

    def collect(self,other):
        self.errors += other.errors

    def reportUnit(self,unit,now):
        result = '%5s %s' % (self.status,self.get_purpose())
        result += ' | %d/%d %s' % (self.unitsConsumed[unit],self.unitsExpected[unit],unit)
        result += ' | %lf%% complete' % (100.0*self.unitsConsumed[unit]/self.unitsExpected[unit])
        if self.startTime is not None:
            result += ' | %lf %s/second' % (self.unitsConsumed[unit]/(now - self.startTime).total_seconds(),unit)
        return result

    def print_arg(self, arg):
        if arg is None:
            return 'None'
        if type(arg) in (list,tuple):
            return '[%s]' % ','.join(self.print_arg(item) for item in arg)
        elif type(arg) in (dict,):
            return '{%s}' % ','.join(f'{key}: {self.print_arg(value)}' for key,value in arg.items())
        elif type(arg).__str__ is not object.__str__:
            return str(arg)
        else:
            return f'{arg.__class__.__name__}@{id(arg)}'

    def __str__(self):
        result = None
        now = self.endTime
        if now is None:
            now = datetime.datetime.now()
        arg_list = []
        if self.args:
            arg_list += [ self.print_arg(arg) for arg in self.args ]
        if self.kwargs:
            arg_list += [ f'{key}={self.print_arg(value)}' for key,value in self.kwargs.items()]
        args_printed = f"({','.join(arg_list)})"
        task_id = f"task{self.id_} " if self.id_ is not None else ""
        result = '%s %s%s%s' % (self.status, task_id, self.get_purpose(), args_printed)
        if self.endTime is not None and self.startTime is not None:
            result += f' {(self.endTime - self.startTime).total_seconds() * 1000.0}ms'
        if self.returnValue is not None:
            result += f' RETURNS {str(self.get_returns())}'
        if self.exception is not None:
            result += f" RAISES  {traceback.format_exception(self.exception['exc_type'], self.exception['exc_value'], self.exception['traceback'])}"
        return result

    def fromJSON(self, obj):
        self.parent_id = int(obj.get('parent_id'), 0) if 'parent_id' in obj else None
        self.id_ = int(obj.get('id'), 0)
        self.purpose = obj.get('purpose')
        self.startTime = arrow.get(obj.get('startTime')).datetime if 'startTime' in obj else None
        self.endTime = arrow.get(obj.get('endTime')).datetime if 'endTime' in obj else None
        self.unitsExpected = obj.get('unitsExpected')
        self.unitsConsumed = obj.get('unitsConsumed')

        self.warnings = obj.get('warnings')
        self.errors = obj.get('errors')
        self.args = obj.get('args')
        self.kwargs = obj.get('kwargs')
        self.returnValue = obj.get('returnValue')
        self.exception = obj.get('exception')
        self.status = obj.get('status')

    def get_purpose(self, limit=160):
        if self.purpose is None:
            return self.purpose
        purpose = str(self.purpose)
        if len(purpose) > limit - 4:
            purpose = f"{purpose[0:limit-4]} ..."
        return purpose

    def get_returns(self, limit=80):
        if self.returnValue is None:
            return self.returnValue
        returnValue = str(self.returnValue)
        if len(returnValue) > limit - 4:
            returnValue = f"{returnValue[0:limit-4]} ..."
        return returnValue

class MethodTask(Task):
    def __init__(self, target,*args, **kwargs):        
        method_name = inspect.currentframe().f_back.f_code.co_name
        Task.__init__(self, purpose=f"{target.__class__.__qualname__}.{method_name}", *args, **kwargs)
        self.target = target
        self.method_name = method_name
