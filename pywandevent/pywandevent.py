
import sys,socket,time, ctypes, os, select, errno

class TimeSpec(ctypes.Structure):
    _fields_ = [
        ('tv_sec', ctypes.c_long),
        ('tv_nsec', ctypes.c_long)
    ]

class PyWandEvent:

    def __init__(self):
        self.read_fds = []
        self.write_fds = []
        self.ex_fds = []

        self.fd_events = {}
        self.timers = []

        librt = ctypes.CDLL('librt.so.1', use_errno=True)
        self.clock_gettime = librt.clock_gettime
        self.clock_gettime.argtypes = [ctypes.c_int, ctypes.POINTER(TimeSpec)]

        self.running = True

    def get_monotonic_time(self):
        t = TimeSpec()
        if self.clock_gettime(4, ctypes.pointer(t)) != 0:
            errno_ = ctypes.get_errno()
            raise OSError(errno_, os.strerror(errno_))
        self.monotonic_time = t.tv_sec + t.tv_nsec * 1e-9

    def add_fd_event(self, sock, evtype, data, callback):
        fd = sock.fileno()
        new_fd_ev = (evtype, sock, data, callback)

        self.fd_events[fd] = new_fd_ev

        if (evtype & 1) == 1:
            self.read_fds.append(fd)
        if (evtype & 2) == 2:
            self.write_fds.append(fd)
        if (evtype & 4) == 4:
            self.ex_fds.append(fd)

    def update_fd_data(self, sock, data):
        fd = sock.fileno()
        if not self.fd_events.has_key(fd):
            print >> sys.stderr, "PyWandEvent: cannot update fd %d - does not exist in fd event list" % (fd)
            return

        old_ev = self.fd_events[fd]
        new_fd_ev = (old_ev[0], sock, data, old_ev[3])
        self.fd_events[fd] = new_fd_ev

    def del_fd_event(self, sock):
        
        fd = sock.fileno()

        if not self.fd_events.has_key(fd):
            print >> sys.stderr, "PyWandEvent: Tried to delete event for fd %d but no event was present!" % (fd) 
            return

        fd_ev = self.fd_events[fd]
        if (fd_ev[0] & 1) == 1:
            self.read_fds = filter(lambda a: a != fd, self.read_fds)
        if (fd_ev[0] & 2) == 2:
            self.write_fds = filter(lambda a: a != fd, self.write_fds)
        if (fd_ev[0] & 4) == 4:
            self.ex_fds = filter(lambda a: a != fd, self.ex_fds)


    def run(self):

        while (self.running):
            self.get_monotonic_time()

            while self.timers != [] and self.timers[0][0] < self.monotonic_time:
                # TODO: Process all expired timers

                pass

            if self.timers != []:
                next_timer = self.timers[0]

                # TODO: calculate select timeout based on next timer
                delay = 0
            else:
                delay = None

            while 1:
                try:
                    if delay == None:
                        active = select.select(self.read_fds, self.write_fds, 
                                self.ex_fds)
                    else:
                        active = select.select(self.read_fds, self.write_fds, 
                            self.ex_fds, delay)
                except select.error, e:
                    if e[0] == errno.EINTR:
                        continue
                    else:
                        print >> sys.stderr, "PyWandEvent: Error in select: %s" % (e[1])
                        return

                break

            
            for fd in active[0]:
                callback = self.fd_events[fd][3]
                
                if (self.fd_events[fd][0] & 1) == 1:
                    callback(fd, 1, self.fd_events[fd][1], 
                            self.fd_events[fd][2])
                if (self.fd_events[fd][0] & 2) == 2:
                    callback(fd, 2, self.fd_events[fd][1],
                            self.fd_events[fd][2])
                if (self.fd_events[fd][0] & 4) == 4:
                    callback(fd, 4, self.fd_events[fd][1],
                            self.fd_events[fd][2])

                

        

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :		
