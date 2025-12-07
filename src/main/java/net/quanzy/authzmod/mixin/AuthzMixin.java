/*
package net.quanzy.authzmod.mixin;

import net.minecraft.client.gui.screen.TitleScreen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(TitleScreen.class)
public class AuthzMixin {
    public static final Logger logger = LoggerFactory.getLogger(AuthzMixin.class);

    @Inject(at = @At("HEAD"), method = "init()V")
    private void init(CallbackInfo info) {
        logger.info("This line is printed by authz mixin!");
    }

}

 */
