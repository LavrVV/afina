#include <afina/coroutine/Engine.h>

#include <setjmp.h>
#include <stdio.h>
#include <cstring>
#include <cassert>

namespace Afina {
namespace Coroutine {

void Engine::Store(context &ctx) {
	char StackPosHere;
	
	ctx.Low = ctx.Hight = &StackPosHere;
	
	if (StackBottom < &StackPosHere)
	{
		ctx.Low = StackBottom;
	} else {
		ctx.Hight = StackBottom;
	}
	
	size_t diff = ctx.Hight - ctx.Low;
	
	if (diff > std::get<1>(ctx.Stack))
	{
		delete []std::get<0>(ctx.Stack);
		std::get<0>(ctx.Stack) = new char[diff];
		std::get<1>(ctx.Stack) = diff;
	}
	memcpy(std::get<0>(ctx.Stack), ctx.Low, diff);
}

void Engine::Restore(context &ctx) {
	char StackPosHere;
	
	if(&StackPosHere >= ctx.Low && &StackPosHere <= ctx.Hight){
		Restore(ctx);
	}
	
	memcpy(ctx.Low, std::get<0>(ctx.Stack), std::get<1>(ctx.Stack));
	longjmp(ctx.Environment, 1);
}

void Engine::yield() {
	context *new_routine = alive;
	
	if (new_routine == nullptr) {
		return;
	}
	
	if (new_routine == cur_routine) {
		new_routine = new_routine->next;
	
	}
	sched(new_routine);
}
void Engine::sched(void *routine_) {
	context *ctx = (context*) routine_;
	
	if (cur_routine == ctx) {
		return;
	
	}
	
	if (setjmp(cur_routine->Environment) == 1) {
		return;
	}
	
	Store(*cur_routine);

	cur_routine = ctx;
	Restore(*cur_routine);
}

} // namespace Coroutine
} // namespace Afina
